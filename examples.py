# from data import Dataset
# from query import *
# from accum import AllAccumulator
from pyspark.sql import SparkSession
import datetime

def timeit(f, times=10):
    a = datetime.datetime.now()
    for i in range(times):
        f()
    b = datetime.datetime.now()

    return (b - a) / times

def main():
    spark = SparkSession.builder.appName("API Examples")\
        .getOrCreate()

    nums = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110]

    def path(i):
        return 'hdfs://147.46.241.90:9010/gaia_parquet/{}.parquet'.format(i)

    for n in nums:
        df = spark.read.format('parquet').load(path(n)).repartition(48)
        df.cache()
        df.count()

    
    def measure():
        for n in nums:
            df = spark.read.format('parquet').load(path(n))
            print(df.count())

    t1 = timeit(measure, times=1)

    print(t1)

    # dataset = Dataset(spark, 'd:\\flights\\blocks2')
    # dataset.load()
    
    # print(dataset.get_json_schema())
    # show test data
    # df = dataset.get_sample_df(0)    
    # df.show(10)
    # df.printSchema()

    # year = dataset.get_field_by_name('YEAR')
    # month = dataset.get_field_by_name('MONTH')
    # arrival_delay = dataset.get_field_by_name('ARR_DELAY')

    # count by one categorical

    
    # query = Frequency1DQuery(year, None, dataset)
    # res = query.get_jobs()[0].run(spark)
    # print(res.collect())

    # count by two categorical

    # job = Frequency2DJob(dataset.samples[0], year, month, None, None, dataset, 1)
    # res = job.run(spark)
    # print(res)

    # sum by 1 categorical
    
    # job = AggregateJob(dataset.samples[0], arrival_delay, year, None, None, dataset)
    # res = job.run(spark)

    # histogram 1d

    # job = Histogram1DJob(dataset.samples[0], year, None, None, dataset)
    # res = job.run(spark)

    # histogram 2d

    # job = Histogram2DJob(dataset.samples[0], year, month, None, None, dataset, 1)
    # res = job.run(spark)

if __name__ == '__main__':
    main()

