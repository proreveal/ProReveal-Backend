import os
import json
import re

from .field import FieldTrait, QuantitativeField

class SparkSample:
    def __init__(self, index, path, df, num_rows):
        self.index = index
        self.path = path
        self.df = df
        self.num_rows = num_rows

class SparkDataset:    
    def __init__(self, backend, path):
        self.backend = backend
        self.path = path
        self.fields = []        

    def load(self):        
        metadata_rdd = self.backend.spark.read.text(self.path + '/metadata.json')
        metadata_json = ''.join([row.value.strip() for row in metadata_rdd.collect()])
        metadata = json.loads(metadata_json)

        self.metadata = metadata

        self.name = self.metadata['source']['name'] or os.path.basename(os.path.normpath(self.path))

        self.fields = []
        for fieldTrait in self.metadata['fields']:
            field = FieldTrait.from_json(fieldTrait)
            self.fields.append(field)

        self.samples = []

        for i, batch in enumerate(self.metadata['source']['batches']):
            path = batch['path']

            abs_path = os.path.join(
                self.path, 
                path
            )

            df = self.get_df(abs_path)

            if 'numRows' in batch:
                sample = SparkSample(i, path, df, batch['numRows'])
            else:
                sample = SparkSample(i, path, df, df.count())

            self.samples.append(sample)            

        
        for fieldTrait in self.metadata['fields']:
            if isinstance(field, QuantitativeField):
                if field.min is None:
                    field.min = field.nice(self.samples[0].df[field.name].min())

                if field.max is None:
                    field.max = field.nice(self.samples[0].df[field.name].max())

                if field.num_bins is None:
                    field.num_bins = QuantitativeField.DEFAULT_NUM_BINS

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
        from pyspark.sql.types import StructType, StructField, StringType
        schema = [StructField(field.name, field.get_pyspark_sql_type()())
            for field in self.fields]
        
        return StructType(schema)
    
    def get_json_schema(self):
        return [field.to_json() for field in self.fields]

    def get_df(self, path):
        df = self.backend.spark.read.option('header', 'false')\
            .schema(self.get_spark_schema())\
            .load(path)

        return df
        