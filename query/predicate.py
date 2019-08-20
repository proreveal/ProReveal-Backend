from dataset import DataType

class Predicate:
    @staticmethod
    def from_json(pred_json, dataset):
        if pred_json is None or len(pred_json) == 0:
            return None

        pred_type = pred_json['type']
    
        if pred_type == 'And':
            return AndPredicate([Predicate.from_json(pred, dataset) for pred in pred_json['predicates']])

        field_data_type = pred_json['field']['dataType']
        field_name = pred_json['field']['name']
        field = dataset.get_field_by_name(field_name)
        
        if pred_type == 'Range':
            return RangePredicate(field, pred_json['start'], pred_json['end'], pred_json['includeEnd'])
        elif pred_type == 'Equal':
            if field_data_type == DataType.String.value:
                return StringEqualPredicate(field, pred_json['expected'])            

            return NumericEqualPredicate(field, pred_json['expected'])

        return None

class NumericEqualPredicate(Predicate):
    def __init__(self, field, expected):
        self.field = field
        self.expected = expected

    def to_sql(self):
        return f'{self.field.name} = {self.expected}'

    def to_json(self):
        return {
            'type': 'Equal',
            'field': self.field.to_json(),
            'expected': self.expected
        }

    def to_lambda(self):
        return lambda x: x[self.field.name] == self.expected

class StringEqualPredicate(Predicate):
    def __init__(self, field, expected):
        self.field = field
        self.expected = expected

    def to_sql(self):
        return f'{self.field.name} = "{self.expected}"'
 
    def to_json(self):
        return {
            'type': 'Equal',
            'field': self.field.to_json(),
            'expected': self.expected
        }

    def to_lambda(self):
        return lambda x: x[self.field.name] == self.expected

class RangePredicate(Predicate):
    def __init__(self, field, start, end, include_end):
        self.field = field
        self.start = start
        self.end = end
        self.include_end = include_end

    def to_sql(self):
        inequality = '<'
        if self.include_end:
            inequality = '<='
        
        return f'({self.field.name} >= {self.start} and {self.field.name} {inequality} {self.end})'

    def to_json(self):
        return {
            'type': 'Range',
            'field': self.field.to_json(),
            'start': self.start, 
            'end': self.end,
            'includeEnd': self.include_end
        }

    def to_lambda(self):
        if self.include_end:
            return lambda x: self.start <= x[self.field.name] and x[self.field.name] <= self.end

        return lambda x: self.start <= x[self.field.name] and x[self.field.name] < self.end

class AndPredicate(Predicate):
    def __init__(self, predicates):
        self.predicates = predicates

    def to_sql(self):
        return ' and '.join([pred.to_sql() for pred in self.predicates])

    def to_json(self):
        if len(self.predicates) == 0:
            return None
        
        return {
            'type': 'And',
            'predicates': [p.to_json() for p in self.predicates]
        }

    def to_lambda(self):
        fs = [p.to_lambda() for p in self.predicates]

        def all(x):
            for f in fs:
                if not f(x):
                    return False
            return True
        
        return all