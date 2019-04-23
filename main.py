import json

import socketio
import eventlet

eventlet.monkey_patch()

from data import Dataset
from query import AggregateQuery
from accum import AllAccumulator
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("API testing")\
     .getOrCreate()

dataset = Dataset(spark, 'd:\\flights\\blocks')
dataset.load()

sio = socketio.Server()
app = socketio.WSGIApp(sio)

queue = []
def bg_emit():
    global queue
    if len(queue) > 0:
        job = queue[0]
        queue = queue[1:]
        res = job.run(spark).collect()
        sio.emit('result', res)

def listen():
    while True:
        bg_emit()
        eventlet.sleep(0)

eventlet.spawn(listen)

@sio.on('query')
def query(sid):
    query = AggregateQuery(AllAccumulator(), None,
        None, dataset, [dataset.get_field_by_name('YEAR')], None)

    jobs = query.get_jobs()
    global queue
    for job in jobs:
        queue.append(job)

        
    sio.emit('result', 'query posted')

@sio.on('connect')
def connected(sid, environ):
    sio.emit('welcome')

if __name__ == '__main__':
    eventlet.wsgi.server(eventlet.listen(('', 7999)), app)