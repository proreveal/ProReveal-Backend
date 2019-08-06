import os
import json
import re

import pandas as pd

from .field import FieldTrait

class LocalSample:
    def __init__(self, index, start, end):
        self.index = index
        self.start = start
        self.end = end
        self.num_rows = end - start

class LocalDataset:    
    def __init__(self, backend, path):
        self.backend = backend
        self.path = path
        self.metadata_path = path.replace('.json', '.schema.json')
    
    def load(self):
        sample_rows = self.backend.config.getint('backend', 'sample_rows')

        with open(self.path, encoding='utf8') as fin:
            self.data = json.load(fin)
        
        self.df = pd.DataFrame.from_records(self.data)

        with open(self.metadata_path, encoding='utf8') as fin:
            self.metadata = json.load(fin)

        self.fields = []
        for field in self.metadata:
            self.fields.append(FieldTrait.from_json(field))
        
        num_rows = len(self.data)
        self.samples = [LocalSample(i, s, min(s + sample_rows, num_rows)) for i, s in enumerate(range(0, num_rows, sample_rows))]
        self.num_rows = num_rows

    def get_field_by_name(self, name):
        for field in self.fields:
            if field.name == name:
                return field
        
        return None
    
    def get_spark_schema(self):
        #     from pyspark.sql.types import StructType, StructField, StringType
        schema = [StructField(field.name, field.get_pyspark_sql_type()())
            for field in self.fields]
        
        return StructType(schema)
    
    def get_json_schema(self):
        return [field.to_json() for field in self.fields]

    def get_sample_df(self, sid):
        df = self.spark.read.format('csv').option('header', 'false')\
            .schema(self.get_spark_schema())\
            .load(self.path + '/' + self.metadata['output_files'][sid]['path'])

        return df
    
    def get_df(self):
        paths = [sample.path for sample in self.samples]

        df = self.spark.read.format('csv').option('header', 'false')\
            .schema(self.get_spark_schema())\
            .load([self.path + '/' + path for path in paths])

        return df