import argparse
import configparser
import logging

import json

import socketio
import eventlet

#eventlet.monkey_patch()

from query import *
from job_queue import JobQueue
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

job_queue = JobQueue()

def run_queue():    
    while True:
        if len(job_queue) > 0 and job_queue.peep().state == JobState.Running:
            job = job_queue.dequeue()
            sio.emit('STATUS/job/start', job.query.to_json())
            res = job.run(spark)
            sio.emit('STATUS/job/end', job.query.to_json())
            sio.emit('result', {                
                'query': job.query.to_json(),
                'job': job.to_json(),
                'result': res
            }) # to 

        eventlet.sleep(0)

forever = eventlet.spawn(run_queue)

@sio.on('connect')
def connect(sid, environ):
    sio.emit('welcome', backend.get_welcome(), to=sid)

@sio.on('disconnect')
def disconnect(sid):
    removed_jobs = job_queue.remove_by_client_socket_id(sid)
    print(sid, 'disconnected')
    print(removed_jobs, 'jobs removed')

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
            'session': session.json()
        }, to=sid)

@sio.on('REQ/schema')
def schema(sid):
    sio.emit('RES/schema', {
        'name': os.path.basename(os.path.normpath(dataset.path)),
        'schema': dataset.get_json_schema(),
        'numRows': dataset.num_rows,
        'numBatches': len(dataset.samples)        
    }, to=sid)

@sio.on('REQ/query')
def query(sid, data):
    query_json = data['query']
    queue_json = data['queue']

    print(f'Incoming query from {sid} {query_json} {queue_json}')
    client_query_id = query_json['id']
    query = Query.from_json(query_json, dataset, sid, client_query_id)

    job_queue.append(query.get_jobs())
    job_queue.reschedule(queue_json['queries'], queue_json['mode'])

    sio.emit('RES/query', {
        'clientQueryId': client_query_id, 
        'queryId': query.id
    }, to=sid)

@sio.on('REQ/query/pause')
def query_pause(sid, data):
    query_json = data['query']
    queue_json = data['queue']
    query_id = query_json['id']
    
    job_queue.pause_by_query_id(query_id)
    job_queue.reschedule(queue_json['queries'], queue_json['mode'])


@sio.on('REQ/query/resume')
def query_resume(sid, data):
    query_json = data['query']
    queue_json = data['queue']
    query_id = query_json['id']
    
    job_queue.resume_by_query_id(query_id)
    job_queue.reschedule(queue_json['queries'], queue_json['mode'])

@sio.on('REQ/query/delete')
def query_delete(sid, query_json):
    query_id = query_json['id']
    
    job_queue.remove_by_query_id(query_id)


@sio.on('REQ/queue/reschedule')
def queue_reschedule(sid, queue_json):
    job_queue.reschedule(queue_json['queries'], queue_json['mode'])

@sio.on('kill')
def kill(sid):
    spark.stop()
    forever.kill()
    sock.close()
    raise SystemExit()
    
if __name__ == '__main__':
    eventlet.wsgi.server(sock, app)