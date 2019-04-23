from .values import *

MAX_VALUE = float('inf')
Undefined = None

class AllAccumulator:
    InitPartialValue = PartialValue(0, 0, 0, MAX_VALUE, -MAX_VALUE, 0)
    InitAccumulatedValue = AccumulatedValue(0, 0, 0, MAX_VALUE, -MAX_VALUE, 0)
    Name = "all"
    AlwaysNonnegative = False

    def reduce(self, a, b):
        if b is Undefined:
            return PartialValue(a.sum, a.ssum, a.count + 1, a.min, a.max, a.nullCount + 1)
        return PartialValue(a.sum + b, a.ssum + b * b, a.count + 1, min(a.min, b), max(a.max, b), a.nullCount)

    def accumulate(self, a, b):
        return AccumulatedValue(a.sum + b.sum, a.ssum + b.ssum, a.count + b.count, min(a.min, b.min), max(a.max, b.max), a.nullCount + b.nullCount)
