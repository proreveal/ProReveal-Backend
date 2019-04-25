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

queue = JobQueue()

def run_queue():    
    while True:
        if len(queue) > 0:
            job = queue.dequeue()
            res = job.run(spark)
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
    queue.remove_by_client_id(sid)

@sio.on('REQ/schema')
def schema(sid):
    sio.emit('RES/schema', {
        'schema': dataset.get_json_schema(),
        'numRows': dataset.num_rows,
        'numBatches': len(dataset.samples)        
    })

@sio.on('REQ/query')
def query(sid, query_json, priority):
    print(f'Incoming query from {sid}')
    query = Query.from_json(query_json, dataset, sid)
    client_query_id = query_json['id']

    queue.append(query.get_jobs())

    sio.emit('RES/query', {
        'clientQueryId': client_query_id, 
        'queryId': query.id
    })

@sio.on('REQ/query/delete')
def query_delete(sid, query_json):
    query_id = query_json['id']
    
    queue.remove_by_query_id(query_id)


@sio.on('kill')
def kill(sid):
    spark.stop()
    forever.kill()
    sock.close()
    raise SystemExit()
    
if __name__ == '__main__':
    eventlet.wsgi.server(sock, app)