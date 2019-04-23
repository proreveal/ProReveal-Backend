@app.route("/")
def hello():    
    nums = sc.parallelize([1, 2, 3, 4])
    result = nums.map(lambda x: x*x).collect()
    return json.dumps(result)


