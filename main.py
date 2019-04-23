# from pyspark.sql import SparkSession
from flask import Flask, request
import json
from flask_socketio import SocketIO, emit

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

@app.route("/")
def hello():    
    nums = sc.parallelize([1, 2, 3, 4])
    result = nums.map(lambda x: x*x).collect()
    return json.dumps(result)

@app.route('/shutdown', methods=['GET'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'
    
@socketio.on('connect')
def connected():
    emit('welcome')

@socketio.on('meesage')
def ss(msg):
    print('received', msg)

if __name__ == '__main__':
    #app.run(host="localhost", port=7999, debug=False, use_reloader=False)
    socketio.run(app, host="0.0.0.0", port=7999, debug=True)
