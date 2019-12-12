from accum import *
from enum import Enum
import pandas as pd
import numpy as np

MAX_VALUE = float('inf')
EMPTY_MAGIC_STRING = 'NANANA'
EMPTY_KEY = -999

class JobState(Enum):
    Running = 'Running'
    Paused = 'Paused'

class Job:
    id = 1

    def __init__(self, index):
        self.id = Job.id
        self.index = index
        self.state = JobState.Running
        Job.id += 1

    def resume(self):
        self.state = JobState.Running
    
    def pause(self):
        self.state = JobState.Paused
        
    def to_json(self):
        return {'id': self.id}

class SelectJob(Job):
    def __init__(self, index, sample, where, query, dataset, limit=100):
        super().__init__(index)

        self.sample = sample
        #self.idx_from = idx_from    
        #self.idx_to = idx_to
        self.where = where
        self.query = query
        self.dataset = dataset        
        self.limit = limit

    def run(self, spark):
        df = self.sample.df

        #idx_from = self.idx_from
        #idx_to = self.idx_to
        
        rows = df.filter(self.where.to_sql()).take(self.limit) #.rdd \
            #.zipWithIndex() \
            #.filter(lambda x: idx_from <= x[1] and x[1] < idx_to) \
            #.map(lambda x: x[0])

        return rows#.collect()        

    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}

class AggregateJob(Job):
    def __init__(self, index, sample, target, grouping, where, query, dataset):
        super().__init__(index)

        self.sample = sample
        self.target = target
        self.grouping = grouping
        self.where = where
        self.query = query
        self.dataset = dataset

    def run_spark(self, spark):
        df = self.sample.df

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


    def run(self):
        """ returns [['A', 10], ['B', 20]]"""

        df = self.sample.df

        if self.where is not None:
            df = df[df.apply(self.where.to_lambda(), axis=1)]

        counts = df.groupby(self.grouping.name)[self.target.name].agg([
            ('sum', 'sum'),
            ('ssum', lambda x: np.sum(np.array(x) ** 2)),
            ('count', lambda x: len([y for y in x if pd.notnull(y)])),
            ('min', 'min'),
            ('max', 'max'),
            ('null_count', lambda x: len([y for y in x if not pd.notnull(y)])),
        ])
        
        counts = [[index, float(row['sum']), float(row['ssum']), float(row['count']),
         float(row['min']), float(row['max']), float(row['null_count'])] for index, row in counts.iterrows()]
            
        return counts

    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}


class Histogram1DJob(Job):
    def __init__(self, index, sample, grouping, bin_spec, where, query, dataset):
        super().__init__(index)

        self.sample = sample
        self.grouping = grouping
        self.bin_spec = bin_spec
        self.where = where
        self.query = query
        self.dataset = dataset
    
    def run_spark(self, spark):        
        bin_start = self.bin_spec.start
        bin_step = self.bin_spec.step()
        num_bins = self.bin_spec.num_bins

        def mapper(value):
            if value[0] is None:
                return (None, )

            x = int((value[0] - bin_start) // bin_step)
            return (max(min(x, num_bins - 1), 0), )
        
        grouping_name = self.grouping.name

        df = self.sample.df
        if self.where is not None:
            df = df.filter(self.where.to_sql())

        rdd = df.rdd.map(lambda row: (row[grouping_name], ))

        counts = list(rdd.map(mapper).countByKey().items())

        return counts

    def run(self):
        bin_start = self.bin_spec.start
        bin_step = self.bin_spec.step()
        num_bins = self.bin_spec.num_bins
        grouping_name = self.grouping.name
        
        df = self.sample.df

        if self.where is not None:
            df = df[df.apply(self.where.to_lambda(), axis=1)]
                
        bins = self.bin_spec.range()
        bins[0]-=1

        counts = pd.cut(df[grouping_name], bins=bins, labels=list(range(num_bins)))
        counts = counts.value_counts()
        counts = [[index, count] for index, count in counts.items()]        

        return counts
        
    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}

class Histogram2DJob(Job):
    def __init__(self, index, sample, grouping1, bin_spec1, grouping2, bin_spec2, where, query, dataset):
        super().__init__(index)

        self.sample = sample
        self.grouping1 = grouping1
        self.bin_spec1 = bin_spec1
        self.grouping2 = grouping2
        self.bin_spec2 = bin_spec2
        self.where = where
        self.query = query
        self.dataset = dataset
    
    def run_spark(self, spark):
        bin_start1 = self.bin_spec1.start
        bin_step1 = self.bin_spec1.step()
        num_bins1 = self.bin_spec1.num_bins

        bin_start2 = self.bin_spec2.start
        bin_step2 = self.bin_spec2.step()
        num_bins2 = self.bin_spec2.num_bins

        def mapper(value):
            if value[0][0] is None:
                x = (None, )
            else:
                x = int((value[0][0] - bin_start1) // bin_step1)
                x = max(min(x, num_bins1 - 1), 0)

            if value[0][1] is None:
                y = (None, )
            else:
                y = int((value[0][1] - bin_start2) // bin_step2)
                y = max(min(y, num_bins2 - 1), 0)

            return ((x, y), )

        grouping_name1 = self.grouping1.name
        grouping_name2 = self.grouping2.name

        df = self.sample.df
        rdd = df.rdd.map(lambda row: ((row[grouping_name1], row[grouping_name2]),))

        # print('count starts')
        counts = list(rdd.map(mapper).countByKey().items())

        return counts

    def run(self):
        bin_start1 = self.bin_spec1.start
        bin_step1 = self.bin_spec1.step()
        num_bins1 = self.bin_spec1.num_bins
        
        bin_start2 = self.bin_spec2.start
        bin_step2 = self.bin_spec2.step()
        num_bins2 = self.bin_spec2.num_bins

        grouping1_name = self.grouping1.name
        grouping2_name = self.grouping2.name
        
        df = self.sample.df

        if self.where is not None:
            df = df[df.apply(self.where.to_lambda(), axis=1)]
                
        bins1 = self.bin_spec1.range()
        bins2 = self.bin_spec2.range()

        bins1[0]-=1
        bins2[0]-=1
        
        df = df.assign(
            x_group=pd.cut(df[grouping1_name], bins=bins1, labels=list(range(num_bins1))),
            y_group=pd.cut(df[grouping2_name], bins=bins2, labels=list(range(num_bins2)))
        )
        
        df = df.assign(xy_key=pd.Categorical(df.filter(items=['x_group', 'y_group']).apply(tuple, 1)))

        counts = df.groupby('xy_key').size()
        counts = [[index, count] for index, count in counts.items()]        
        return counts

    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}

class Frequency1DJob(Job):
    def __init__(self, index, sample, grouping, where, query, dataset):
        super().__init__(index)

        self.sample = sample
        self.grouping = grouping
        self.where = where
        self.query = query
        self.dataset = dataset        

    def run_spark(self, spark):
        df = self.sample.df

        if self.where is not None:
            df = df.filter(self.where.to_sql())

        counts = df.groupBy(self.grouping.name).count().collect()
        return counts

    def run(self):
        """ returns [['A', 10], ['B', 20]]"""

        df = self.sample.df
        grouping = self.grouping.name

        if self.where is not None:
            df = df[df.apply(self.where.to_lambda(), axis=1)]
        
        df.loc[:, grouping] = df[grouping].fillna(EMPTY_MAGIC_STRING)

        counts = df.groupby(self.grouping.name).size()
        
        counts = [[index if index != EMPTY_MAGIC_STRING else np.nan, count] for index, count in counts.items()]
        return counts
        
    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}


class Frequency2DJob(Job):
    def __init__(self, index, sample, grouping1, grouping2, where, query, dataset):
        super().__init__(index)

        self.sample = sample
        self.grouping1 = grouping1
        self.grouping2 = grouping2
        self.where = where
        self.query = query
        self.dataset = dataset

    def run_spark(self, spark):
        df = self.sample.df

        if self.where is not None:
            df = df.filter(self.where.to_sql())

        counts = df.groupBy(self.grouping1.name, self.grouping2.name).count().collect()

        return counts

    def run(self):
        """ returns [['A', 10], ['B', 20]]"""

        df = self.sample.df

        if self.where is not None:
            df = df[df.apply(self.where.to_lambda(), axis=1)]

        counts = df.groupby([self.grouping1.name, self.grouping2.name]).size()
        counts = [[index, count] for index, count in counts.items()]        
        return counts

    def to_json(self):
        return {'id': self.id, 'numRows': self.sample.num_rows}
