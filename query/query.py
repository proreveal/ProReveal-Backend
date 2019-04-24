import random

from .job import Frequency1DJob

class Query:
    id = 1

    def __init__(self, shuffle):
        self.id = Query.id
        self.shuffle = shuffle
        Query.id += 1

# class AggregateQuery:
#     def __init__(self, accumulator, target, dataset, group_by, where):
#         self.accumulator = accumulator
#         self.target = target
#         self.dataset = dataset
#         self.group_by = group_by
#         self.where = where


#     def get_jobs(self):
#         jobs = []

#         for sample in self.dataset.samples:
#             jobs.append(AggregateJob(
#                 self.accumulator,
#                 self.target,
#                 self.dataset,
#                 self.group_by,
#                 self.where,
#                 self,
#                 sample               
#             ))

#         return jobs


class Frequency1DQuery(Query):
    def __init__(self, group_by, where, dataset, shuffle=True):
        
        super().__init__(shuffle)

        self.group_by = group_by
        self.where = where
        self.dataset = dataset

    def get_jobs(self):
        jobs = []
        
        for sample in self.dataset.samples:
            jobs.append(Frequency1DJob(
                sample, self.group_by, self.where, self, self.dataset
            ))

        if self.shuffle:
            random.shuffle(jobs)

        return jobs

        