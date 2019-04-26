from accum import *
from enum import Enum

MAX_VALUE = float('inf')
EMPTY_KEY = -999

class JobState(Enum):
    Running = 'Running'
    Paused = 'Paused'

class Job:
    id = 1

    def __init__(self, client_id):
        self.id = Job.id
        self.state = JobState.Running
        Job.id += 1

        self.client_id = client_id

    def resume(self):
        self.state = JobState.Running
    
    def pause(self):
        self.state = JobState.Paused
        
    def to_json(self):
        return {'id': self.id}

class AggregateJob(Job):
    def __init__(self, sample, target, grouping, where, query, dataset, client_id):
        super().__init__(client_id)

        self.sample = sample
        self.target = target
        self.grouping = grouping
        self.where = where
        self.query = query
        self.dataset = dataset

    def run(self, spark):
        df = self.dataset.get_sample_df(self.sample.index)

        if self.where is not None:
            df = df.filter(self.where.to_sql())

        target_name = self.target.name
        grouping_name = self.grouping.name

        # no null value
        # (sum, ssum, count, min, max)
        def create_combiner(value):
            return (value, value * value, 1, value, value)
        
        def merge_value(x, value):
            sum, ssum, count, xmin, xmax = x
            return (sum + value, \
                ssum + value * value, \
                count + 1, \
                min(xmin, value), \
                max(xmax, value))
        
        def merge_combiner(x, y):
            return (x[0] + y[0], x[1] + y[1], x[2] + y[2], min(x[3], y[3]), max(x[4], y[4]))

        rdd = df.rdd.map(lambda row: (row[grouping_name], row[target_name]))

        result = rdd.filter(lambda pair: pair[1] is not None) \
                .combineByKey(create_combiner, merge_value, merge_combiner) \
                .collectAsMap()

        null_counts = rdd.filter(lambda pair: pair[1] is None) \
                .countByKey()

        result_keys = set(result.keys())
        null_count_keys = set(null_counts.keys())

        for key in result_keys:
            if key in null_counts:
                result[key] = result[key] + (0, )
            else:
                result[key] = result[key] + (null_counts[key], )
        
        for key in null_count_keys - result_keys:
            result[key] = (0, 0, 0, 0, 0, null_counts[key])
        
        return [(key, ) + res for key, res in result.items()]

    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}

class Frequency1DJob(Job):
    def __init__(self, sample, grouping, where, query, dataset, client_id):
        super().__init__(client_id)

        self.sample = sample
        self.grouping = grouping
        self.where = where
        self.query = query
        self.dataset = dataset        

    def run(self, spark):
        df = self.dataset.get_sample_df(self.sample.index)

        if self.where is not None:
            df = df.filter(self.where.to_sql())

        counts = df.groupBy(self.grouping.name).count().collect()
        return counts

    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}


class Frequency2DJob(Job):
    def __init__(self, sample, grouping1, grouping2, where, query, dataset, client_id):
        super().__init__(client_id)

        self.sample = sample
        self.grouping1 = grouping1
        self.grouping2 = grouping2
        self.where = where
        self.query = query
        self.dataset = dataset

    
    def run(self, spark):
        df = self.dataset.get_sample_df(self.sample.index)

        if self.where is not None:
            df = df.filter(self.where.to_sql())

        counts = df.groupBy(self.grouping1.name, self.grouping2.name).count().collect()

        return counts

    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}

class Histogram1DJob(Job):
    def __init__(self, sample, grouping, where, query, dataset, client_id):
        super().__init__(client_id)

        self.sample = sample
        self.grouping = grouping
        self.where = where
        self.query = query
        self.dataset = dataset
    
    def run(self, spark):        
        bin_start = 0
        bin_size = 1
        num_bins = 10

        def mapper(value):
            if value[0] is None:
                return (EMPTY_KEY, )

            x = (value[0] - bin_start) // bin_size
            return (max(min(x, num_bins - 1), 0), )
        
        grouping_name = self.grouping.name

        df = self.dataset.get_sample_df(self.sample.index)
        rdd = df.rdd.map(lambda row: (row[grouping_name], ))

        result = rdd.map(mapper).countByKey()
        print(result)

    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}

class Histogram2DJob(Job):
    def __init__(self, sample, grouping1, grouping2, where, query, dataset, client_id):
        super().__init__(client_id)

        self.sample = sample
        self.grouping1 = grouping1
        self.grouping2 = grouping2
        self.where = where
        self.query = query
        self.dataset = dataset
    
    def run(self, spark):
        bin_start1 = 0
        bin_size1 = 1
        num_bins1 = 10

        bin_start2 = 0
        bin_size2 = 1
        num_bins2 = 10

        def mapper(value):
            if value[0][0] is None:
                x = EMPTY_KEY
            else:
                x = (value[0][0] - bin_start1) // bin_size1
                x = max(min(x, num_bins1 - 1), 0)

            if value[0][1] is None:
                y = EMPTY_KEY
            else:
                y = (value[0][1] - bin_start2) // bin_size2
                y = max(min(y, num_bins2 - 1), 0)

            return ((x, y), )

        grouping_name1 = self.grouping1.name
        grouping_name2 = self.grouping2.name

        df = self.dataset.get_sample_df(self.sample.index)
        rdd = df.rdd.map(lambda row: ((row[grouping_name1], row[grouping_name2]), ))

        result = rdd.map(mapper).countByKey()

        print(result)

    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}