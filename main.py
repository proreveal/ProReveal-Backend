import argparse
import configparser
import logging

import json

import socketio
import eventlet

#eventlet.monkey_patch()

from query import *
from session import Session
from backend import LocalBackend, SparkBackend

import os

version = '0.2.0'

parser = argparse.ArgumentParser()
parser.add_argument('config', help='A configuration file')
args = parser.parse_args()

config = configparser.ConfigParser()

config.read(args.config)

config.add_section('server')
config.set('server', 'version', version)

if config['backend'].get('type', LocalBackend.config_name) == SparkBackend.config_name:
    logging.info('Using a Spark backend')
    backend = SparkBackend(config)
else:
    logging.info('Using a local backend')
    backend = LocalBackend(config)
    
dataset = backend.load(config['backend']['dataset'])

#print(dataset.get_spark_schema())
#dataset.get_sample_df(0).show()

sio = socketio.Server(cors_allowed_origins='*')
app = socketio.WSGIApp(sio)
sock = eventlet.listen(('', 7999))

sessions = []
test_session = Session()
test_session.code = 'ABC'
sessions.append(test_session)

def get_session_by_sid(sid):
    for ses in sessions:
        if sid in ses.sids:
            return ses
    
    return None

def run_queue():    
    while True:
        for session in sessions:
            job_queue = session.job_queue

            if len(job_queue) > 0 and job_queue.peep().state == JobState.Running:
                job = job_queue.dequeue()
                query = job.query

                sio.emit('STATUS/job/start', {'id': query.id, 
                    'numOngoingBlocks': 1, 
                    'numOngoingRows': job.sample.num_rows},
                    room=session.code)

                res = backend.run(job) # unified format, [[a, 1], [b, 2]]
                query.accumulate(res)
                query.num_processed_blocks += 1
                query.num_processed_rows += job.sample.num_rows
                query.last_updated = now()

                eventlet.sleep(1)

                sio.emit('STATUS/job/end', {'id': query.id},
                    room=session.code)

                sio.emit('result', { 
                    'query': job.query.to_json()
                }, room=session.code)

                if query.done():
                    sio.emit('STATUS/queries', session.query_state_to_json(), room=session.code)

        eventlet.sleep(1)

forever = eventlet.spawn(run_queue)

@sio.on('connect')
def connect(sid, environ):
    sio.emit('welcome', backend.get_welcome(), to=sid)

@sio.on('disconnect')
def disconnect(sid):
    for ses in sessions:
        ses.leave_sid(sid)
        sio.leave_room(sid, ses.code)

    # removed_jobs = job_queue.remove_by_client_socket_id(sid)
    # print(sid, 'disconnected')
    # print(removed_jobs, 'jobs removed')

@sio.on('REQ/restore')
def restore(sid, data):
    code = data['code'].upper()
    
    session = list(filter(lambda x: x.code == code, sessions))
    if len(session) == 0:
        sio.emit('RES/restore', {
            'success': False
        }, to=sid)
    else:
        session = session[0]
        sio.emit('RES/restore', {
            'success': True,
            'session': session.to_json(),
            'metadata': {
                'name': os.path.basename(os.path.normpath(dataset.path)),
                'schema': dataset.get_json_schema(),
                'numRows': dataset.num_rows,
                'numBatches': len(dataset.samples)        
            }
        }, to=sid)

        for ses in sessions:
            ses.leave_sid(sid)
            sio.leave_room(sid, ses.code)

        session.enter_sid(sid)
        sio.enter_room(sid, session.code)

@sio.on('REQ/login')
def login(sid, data):
    code = data['code'].upper()
    
    session = list(filter(lambda x: x.code == code, sessions))
    if len(session) == 0:
        sio.emit('RES/login', {
            'success': False
        }, to=sid)
    else:
        session = session[0]
        sio.emit('RES/login', {
            'success': True,
            'code': session.code
        }, to=sid)

@sio.on('REQ/query')
def query(sid, data):
    session = get_session_by_sid(sid)
    if session is None:
        return

    query_json = data['query']

    print(f'Incoming query from {sid} {query_json}')
    query = Query.from_json(query_json, dataset)

    session.add_query(query)

    sio.emit('RES/query', {'query': query.to_json() }, room=session.code)
    sio.emit('STATUS/queries', session.query_state_to_json(), room=session.code)

@sio.on('REQ/safeguard')
def safeguard(sid, data):
    session = get_session_by_sid(sid)
    if session is None:
        return

    sg_json = data['safeguard']

    print(f'Incoming safeguard from {sid} {sg_json}')
    
    session.add_safeguard(sg_json)

    sio.emit('RES/safeguard', {'safeguard': sg_json}, room=session.code)

@sio.on('REQ/query/pause')
def query_pause(sid, data):
    session = get_session_by_sid(sid)
    if session is None:
        return

    query_json = data['query']
    query_id = query_json['id']

    if session.get_query(query_id) is not None:
        session.pause_query(session.get_query(query_id))

    sio.emit('STATUS/queries', session.query_state_to_json(), room=session.code)
    

@sio.on('REQ/query/resume')
def query_resume(sid, data):
    session = get_session_by_sid(sid)
    if session is None:
        return

    query_json = data['query']
    query_id = query_json['id']
    
    if session.get_query(query_id) is not None:
        session.resume_query(session.get_query(query_id))

    sio.emit('STATUS/queries', session.query_state_to_json(), room=session.code)        

@sio.on('REQ/query/remove')
def query_remove(sid, query_json):
    session = get_session_by_sid(sid)
    if session is None:
        return

    query_id = query_json['id']
    if session.get_query(query_id) is not None:
        session.remove_query(session.get_query(query_id))

    sio.emit('STATUS/queries', session.query_state_to_json(), room=session.code)        

@sio.on('REQ/query/reorder')
def query_reorder(sid, data):
    session = get_session_by_sid(sid)
    if session is None:
        return

    order = data['order']
    print(order)

    session.reorder(order)
    session.reschedule()
    sio.emit('STATUS/queries', session.query_state_to_json(), room=session.code)

@sio.on('REQ/queue/reschedule')
def queue_reschedule(sid, data):
    session = get_session_by_sid(sid)
    if session is None:
        return

    if 'alternate' in data:
        alternate = data['alternate']
        session.alternate = alternate

    session.reschedule()
    sio.emit('STATUS/queries', session.query_state_to_json(), room=session.code)

@sio.on('REQ/safeguard/remove')
def safeguard(sid, data):
    session = get_session_by_sid(sid)
    if session is None:
        return

    sg_json = data['safeguard']
    
    session.remove_safeguard(sg_json)

    sio.emit('STATUS/safeguards', session.safeguards_to_json(), room=session.code)

@sio.on('kill')
def kill(sid):
    backend.stop()
    forever.kill()
    sock.close()
    raise SystemExit()
    
if __name__ == '__main__':
    eventlet.wsgi.server(sock, app)