import random

from .job import *
from .predicate import Predicate

class Query:
    id = 1
    
    def __init__(self, client_id, shuffle):
        self.id = f'Query{Query.id}'
        self.client_id = client_id
        self.shuffle = shuffle
        Query.id += 1

    @staticmethod
    def from_json(json, dataset, client_id):
        type_string = json['type']
        where_string = json['where']

        where = None
        
        if where_string is not None and len(where_string) > 0:
            where = Predicate.from_json(where_string)

        if type_string == Frequency1DQuery.name:
            grouping = json['grouping']['name']

            return Frequency1DQuery(dataset.get_field_by_name(grouping), where, dataset, client_id)

        elif type_string == Frequency2DQuery.name:
            grouping1 = dataset.get_field_by_name(json['grouping1']['name'])
            grouping2 = dataset.get_field_by_name(json['grouping2']['name'])

            return Frequency2DQuery(grouping1, grouping2, where, dataset, client_id)
        
        elif type_string == AggregateQuery.name:
            target = dataset.get_field_by_name(json['target']['name'])
            grouping = dataset.get_field_by_name(json['grouping']['name'])

            return AggregateQuery(target, grouping, where, dataset, client_id)
        return 
    
    def to_json(self):
        return {'id': self.id}

class AggregateQuery(Query):
    name = "AggregateQuery"

    def __init__(self, target, grouping, where, dataset, client_id, shuffle=True):        
        super().__init__(client_id, shuffle)

        self.target = target
        self.grouping = grouping
        self.where = where
        self.dataset = dataset

    def get_jobs(self):
        jobs = []
        
        for sample in self.dataset.samples:
            jobs.append(AggregateJob(
                sample, self.target, self.grouping, self.where, 
                self, self.dataset, self.client_id
            ))

        if self.shuffle:
            random.shuffle(jobs)

        return jobs

class Frequency1DQuery(Query):
    name = "Frequency1DQuery"

    def __init__(self, grouping, where, dataset, client_id, shuffle=True):        
        super().__init__(client_id, shuffle)

        self.grouping = grouping
        self.where = where
        self.dataset = dataset

    def get_jobs(self):
        jobs = []
        
        for sample in self.dataset.samples:
            jobs.append(Frequency1DJob(
                sample, self.grouping, self.where, self, self.dataset, self.client_id
            ))

        if self.shuffle:
            random.shuffle(jobs)

        return jobs

class Frequency2DQuery(Query):
    name = "Frequency2DQuery"

    def __init__(self, grouping1, grouping2, where, dataset, client_id, shuffle=True):        
        super().__init__(client_id, shuffle)

        self.grouping1 = grouping1
        self.grouping2 = grouping2
        self.where = where
        self.dataset = dataset

    def get_jobs(self):
        jobs = []
        
        for sample in self.dataset.samples:
            jobs.append(Frequency2DJob(
                sample, self.grouping1, self.grouping2, self.where, self, self.dataset, self.client_id
            ))

        if self.shuffle:
            random.shuffle(jobs)

        return jobs

        