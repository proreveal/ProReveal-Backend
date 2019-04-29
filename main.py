import json

import socketio
import eventlet

eventlet.monkey_patch()

from pyspark.sql import SparkSession

from data import Dataset
from query import *
from job_queue import JobQueue

version = '0.1.0'
spark = SparkSession.builder.appName(f'ProReveal Spark Engine {version}')\
     .getOrCreate()

dataset = Dataset(spark, 'd:\\flights\\blocks2')
dataset.load()

sio = socketio.Server()
app = socketio.WSGIApp(sio)
sock = eventlet.listen(('', 7999))

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
            })
    
        eventlet.sleep(0)

forever = eventlet.spawn(run_queue)

@sio.on('connect')
def connect(sid, environ):
    sio.emit('welcome', f'Welcome from ProReveal Spark Engine {version}')

@sio.on('disconnect')
def disconnect(sid):
    job_queue.remove_by_client_id(sid)

@sio.on('REQ/schema')
def schema(sid):
    sio.emit('RES/schema', {
        'schema': dataset.get_json_schema(),
        'numRows': dataset.num_rows,
        'numBatches': len(dataset.samples)        
    })

@sio.on('REQ/query')
def query(sid, data):
    query_json = data['query']
    queue_json = data['queue']

    print(f'Incoming query from {sid} {query_json} {queue_json}')
    query = Query.from_json(query_json, dataset, sid)
    client_query_id = query_json['id']
    query.client_id = client_query_id

    job_queue.append(query.get_jobs())
    job_queue.reschedule(queue_json['queries'], queue_json['mode'])

    sio.emit('RES/query', {
        'clientQueryId': client_query_id, 
        'queryId': query.id
    })

@sio.on('REQ/query/pause')
def query_pause(sid, data):
    query_json = data['query']
    queue_json = data['queue']
    query_id = query_json['id']
    
    job_queue.pause_by_query_id(query_id)
    job_queue.reschedule(queue_json['queries'], queue_json['mode'])


@sio.on('REQ/query/resume')
def query_pause(sid, data):
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