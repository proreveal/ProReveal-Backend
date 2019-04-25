import os
import json

from pyspark.sql.types import StructType, StructField
from .field import FieldTrait

class Sample:
    def __init__(self, index, path, num_rows):
        self.index = index
        self.path = path
        self.num_rows = num_rows

class Dataset:    
    def __init__(self, spark, path):
        self.spark = spark
        self.path = path
        self.fields = []        
    
    def load(self):
        self.metadata = json.load(open(os.path.join(self.path, 'metadata.json')))

        self.fields = []
        for field in self.metadata['header']:
            self.fields.append(FieldTrait.from_json(field))
        
        self.samples = [Sample(i, sample['path'], sample['num_rows']) for i, sample in enumerate(self.metadata['output_files'])]

        num_rows = 0
        for sample in self.samples:
            num_rows += sample.num_rows

        self.num_rows = num_rows        

    def get_field_by_name(self, name):
        for field in self.fields:
            if field.name == name:
                return field
        
        return None
    
    def get_spark_schema(self):
        schema = [StructField(field.name, field.get_pyspark_sql_type()())
            for field in self.fields]
        
        return StructType(schema)
    
    def get_json_schema(self):
        schema = []
        for field in self.fields:
            schema.append({
                'name': field.name,
                'vlType': field.vl_type.value,
                'dataType': field.data_type.value
            })
        
        return schema

    def get_sample_df(self, sid):
        df = self.spark.read.format('csv').option('header', 'false')\
            .schema(self.get_spark_schema())\
            .load(self.metadata['output_files'][sid]['path'])

        return df
    