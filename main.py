import json
import time

import socketio
import engineio
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

# def shutdown_server():
#     spark.stop()

#     func = request.environ.get('werkzeug.server.shutdown')    
#     if func is None:
#         socketio.stop()
#         raise RuntimeError('Not running with the Werkzeug Server')
#     func()


# @app.route("/")
# def hello():    
#     nums = sc.parallelize([1, 2, 3, 4])
#     result = nums.map(lambda x: x*x).collect()
#     return json.dumps(result)

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
        eventlet.sleep(0.1)

eventlet.spawn(listen)

@sio.on('query')
def query(sid):
    sio.emit('result', 123)
    query = AggregateQuery(AllAccumulator(), None,
        None, dataset, [dataset.get_field_by_name('YEAR')], None)

    jobs = query.get_jobs()
    global queue
    for job in jobs:
        queue.append(job)

    pass
    # for job in jobs[:30]:
    #     res = job.run(spark).collect()
    #     emit('result', res)
    #     time.sleep(0.1)

@sio.on('connect')
def connected(sid, environ):
    sio.emit('welcome')

if __name__ == '__main__':
    eventlet.wsgi.server(eventlet.listen(('', 7999)), app)

    #app.run(host="localhost", port=7999, debug=False, use_reloader=False)
    # app.wsgi_app = socketio.Middleware(sio, app.wsgi_app)
    # app.run(threaded=True, host="0.0.0.0", port=7999, debug=False, use_reloader=False)
    # socketio.run(app, host="0.0.0.0", port=7999, debug=False)
