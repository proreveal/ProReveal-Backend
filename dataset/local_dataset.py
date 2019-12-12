import os
import json
import re

import pandas as pd

from .field import FieldTrait, QuantitativeField

class LocalSample:
    def __init__(self, index, df):
        self.index = index
        self.df = df
        self.num_rows = len(df.index)

class LocalDataset:    
    def __init__(self, backend, path):
        self.backend = backend
        self.metadata_path = os.path.join(path, 'metadata.json')

        self.path = path        
    
    def load(self):        
        with open(self.metadata_path, encoding='utf8') as fin:
            self.metadata = json.load(fin)

        self.name = self.metadata['source']['name'] or os.path.basename(os.path.normpath(self.path))

        if self.metadata['source']['path']:
            # if a dataset is a single file, read and split the dataset by sample_rows

            abs_source_path = os.path.abspath(os.path.join(
                self.path, 
                self.metadata['source']['path']
            ))

            sample_rows = self.backend.config.getint('backend', 'sample_rows')

            with open(abs_source_path, encoding='utf8') as fin:
                data = json.load(fin)
        
            df = pd.DataFrame.from_records(data)
            num_rows = len(df.index)

            self.samples = [LocalSample(i, df.iloc[s:min(s + sample_rows, num_rows)]) for i, s in enumerate(range(0, num_rows, sample_rows))]

        self.fields = []
        for fieldTrait in self.metadata['fields']:
            field = FieldTrait.from_json(fieldTrait)

            if isinstance(field, QuantitativeField):
                if field.min is None:
                    field.min = field.nice(self.samples[0].df[field.name].min())

                if field.max is None:
                    field.max = field.nice(self.samples[0].df[field.name].max())

                if field.num_bins is None:
                    field.num_bins = QuantitativeField.DEFAULT_NUM_BINS

            self.fields.append(field)
                
        self.num_rows = num_rows

    def get_field_by_name(self, name):
        for field in self.fields:
            if field.name == name:
                return field
        
        return None
    
    def get_json_schema(self):
        return [field.to_json() for field in self.fields]
