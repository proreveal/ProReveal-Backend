from pyspark.sql import SparkSession
from flask import Flask
import json
from flask import request

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

app = Flask(__name__)
spark = None
sc = None

def init_spark():
    global spark, sc
    print("asdfajhfoiwejfoiwefjoiewf", spark)
    if spark is None:
        spark = SparkSession.builder.appName("ProReveal-Server").getOrCreate()
        sc = spark.sparkContext
        

@app.route("/")
def hello():    
    nums = sc.parallelize([1, 2, 3, 4])
    result = nums.map(lambda x: x*x).collect()
    return json.dumps(result)

@app.route('/shutdown', methods=['GET'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'
    
if __name__ == '__main__':
    init_spark()

    app.run(host="localhost", port=7999, debug=False, use_reloader=False)
