import os
import json

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

class Dataset:
    def __init__(self, spark, path):
        self.spark = spark
        self.path = path

    
    def load(self):
        self.metadata = json.load(open(os.path.join(self.path, 'metadata.json')))
    
    def schema(self):
        schema = []
        for field in self.metadata['header']:
            name = field['name']
            data_type = field['dataType']
            field_type = None

            if data_type == 'Integer':
                field_type = IntegerType()
            elif data_type == 'String':
                field_type = StringType()
            elif data_type == 'Real':
                field_type = DoubleType()

            schema.append(StructField(name, field_type))
        
        return StructType(schema)
    
    def get_sample_df(self, sid):
        df = self.spark.read.format('csv').option('header', 'false').schema(self.schema()).load(self.metadata['output_files'][sid]['output_path'])

        return df
    