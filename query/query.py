from .job import AggregateJob

class AggregateQuery:
    def __init__(self, accumulator, approximator,
        target, dataset, group_by, where):
        self.accumulator = accumulator
        self.target = target
        self.dataset = dataset
        self.group_by = group_by
        self.where = where


    def get_jobs(self):
        jobs = []

        for sample in self.dataset.samples:
            jobs.append(AggregateJob(
                self.accumulator,
                self.target,
                self.dataset,
                self.group_by,
                self.where,
                self,
                sample               
            ))

        return jobs