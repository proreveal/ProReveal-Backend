from .values import *

MAX_VALUE = float('inf')
Undefined = None

class AllAccumulator:
    InitAggregateValue = AggregateValue(0, 0, 0, MAX_VALUE, -MAX_VALUE, 0)
    Name = "all"
    AlwaysNonnegative = False

    def reduce(self, a, b):
        if b is Undefined:
            return AggregateValue(a.sum, a.ssum, a.count + 1, a.min, a.max, a.null_count + 1)
        return AggregateValue(a.sum + b, a.ssum + b * b, a.count + 1, min(a.min, b), max(a.max, b), a.null_count)

    def accumulate(self, a, b):
        return AggregateValue(a.sum + b.sum, a.ssum + b.ssum, a.count + b.count, min(a.min, b.min), max(a.max, b.max), a.null_count + b.null_count)
