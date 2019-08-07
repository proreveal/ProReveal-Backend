from dataset import *

class BackendBase:
    def __init__(self, config):
        self.config = config
        
class SparkBackend(BackendBase):    
    config_name = 'spark'

    def __init__(self, config):
        super().__init__(config)

        import pyspark
        from pyspark.sql import SparkSession        

        self.pyspark = pyspark

        version = config['server']['version']
        spark = SparkSession.builder.appName(f'ProReveal Spark Engine {version}')\
            .getOrCreate()
        self.spark = spark
        
    def load(self, path):
        dataset = SparkDataset(self, path)
        dataset.load()
        return dataset
    
    def get_welcome(self):
        config = self.config
        spark = self.spark
        return {
            'version': config['server']['version'],
            'backend': 'spark',
            'sparkVersion': spark.version,
            'master': spark.sparkContext.master,
            'uiWebUrl': spark.sparkContext.uiWebUrl
        }

    def run(self, job):
        return job.run(self.spark)

    def stop(self):
        self.spark.stop()
            
class LocalBackend(BackendBase):
    config_name = 'local'

    def __init__(self, config):
        super().__init__(config)

    def load(self, path):
        dataset = LocalDataset(self, path)
        dataset.load()
        return dataset
    
    def get_welcome(self):
        config = self.config
        return {
            'version': config['server']['version'],
            'backend': 'local'
        }

    def run(self, job):
        return job.run()

    def stop(self):
        return