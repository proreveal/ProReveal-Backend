from data import Dataset
from query import AggregateQuery
from accum import AllAccumulator
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("API testing")\
        .getOrCreate()

    dataset = Dataset(spark, 'd:\\flights\\blocks')
    dataset.load()
    df = dataset.get_sample_df(0)
    
    # df.show(10)
    # df.printSchema()

    # count by categorical

    query = AggregateQuery(AllAccumulator(), None,
        None, dataset, [dataset.get_field_by_name('YEAR')], None)

    jobs = query.get_jobs()

    res = jobs[0].run(spark)
    print(res.collect())


    
if __name__ == '__main__':
    main()

