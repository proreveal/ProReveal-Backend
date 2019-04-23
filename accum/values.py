class PartialValue:
    def __init__(self, sum, ssum, count, min, max, nullCount):
        self.sum = sum
        self.ssum = ssum
        self.count = count
        self.min = min
        self.max = max
        self.nullCount = nullCount

class AccumulatedValue:
    def __init__(self, sum, ssum, count, min, max, nullCount):
        self.sum = sum
        self.ssum = ssum
        self.count = count
        self.min = min
        self.max = max
        self.nullCount = nullCount

    def clone(self):
        return AccumulatedValue(self.sum, self.ssum, self.count, self.min, self.max, self.nullCount)

    def to_partial(self):
        return PartialValue(self.sum, self.ssum, self.count, self.min, self.max, self.nullCount)


        