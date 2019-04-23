class Job:
    id = 1

class AggregateJob(Job):
    def __init__(self, accumulator, target, dataset, group_by, where, query, sample):
        self.id = Job.id
        Job.id += 1

        self.accumulator = accumulator
        self.target = target
        self.dataset = dataset
        self.group_by = group_by
        self.where = where
        self.query = query
        self.sample = sample


    def run(self, spark):
        df = self.dataset.get_sample_df(self.sample.index)
        return df.groupBy(self.group_by[0].name).count()
