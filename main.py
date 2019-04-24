import json

import socketio
import eventlet

eventlet.monkey_patch()

from data import Dataset
from pyspark.sql import SparkSession

version = '0.1.0'
spark = SparkSession.builder.appName(f'ProReveal Spark Engine {version}')\
     .getOrCreate()

dataset = Dataset(spark, 'd:\\flights\\blocks')
dataset.load()

sio = socketio.Server()
app = socketio.WSGIApp(sio)
sock = eventlet.listen(('', 7999))

queue = []

def run_queue():    
    global queue
    
    while True:
        if len(queue) > 0:
            job = queue[0]
            queue = queue[1:]
            res = job.run(spark).collect()
            sio.emit('result', res)
    
        eventlet.sleep(0)

forever = eventlet.spawn(run_queue)

# @sio.on('query')
# def query(sid):
#     query = AggregateQuery(AllAccumulator(), None,
#         None, dataset, [dataset.get_field_by_name('YEAR')], None)

#     jobs = query.get_jobs()
#     global queue
#     for job in jobs:
#         queue.append(job)
        
#     sio.emit('result', 'query posted')

@sio.on('connect')
def connect(sid, environ):
    sio.emit('welcome', f'Welcome from ProReveal Spark Engine {version}')

@sio.on('REQ/schema')
def req_schema(sid):
    sio.emit('RES/schema', dataset.get_json_schema())

@sio.on('kill')
def kill(sid):
    spark.stop()
    forever.kill()
    sock.close()
    raise SystemExit()
    
if __name__ == '__main__':
    eventlet.wsgi.server(sock, app)