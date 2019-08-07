class AggregateValue:
    def __init__(self, sum, ssum, count, min, max, null_count):
        self.sum = sum
        self.ssum = ssum
        self.count = count
        self.min = min
        self.max = max
        self.null_count = null_count

    def clone(self):
        return AggregateValue(self.sum, self.ssum, self.count, self.min, self.max, self.null_count)

    def to_tuple(self):
        return (self.sum, self.ssum, self.count, self.min, self.max, self.null_count)