from dataset import DataType

class Predicate:
    @staticmethod
    def from_json(pred_json):
        pred_type = pred_json['type']
    
        if pred_type == 'and':
            return AndPredicate([Predicate.from_json(pred) for pred in pred_json['predicates']])

        field_data_type = pred_json['field']['dataType']
        field_name = pred_json['field']['name']
        
        if pred_type == 'range':
            return RangePredicate(field_name, pred_json['start'], pred_json['end'], pred_json['includeEnd'])
        elif pred_type == 'equal':
            if field_data_type == DataType.String.value:
                return StringEqualPredicate(field_name, pred_json['expected'])            

            return NumericEqualPredicate(field_name, pred_json['expected'])

        return None

class NumericEqualPredicate(Predicate):
    def __init__(self, field_name, expected):
        self.field_name = field_name
        self.expected = expected

    def to_sql(self):
        return f'{self.field_name} = {self.expected}'

class StringEqualPredicate(Predicate):
    def __init__(self, field_name, expected):
        self.field_name = field_name
        self.expected = expected

    def to_sql(self):
        return f'{self.field_name} = "{self.expected}"'
 
class RangePredicate(Predicate):
    def __init__(self, field_name, start, end, include_end):
        self.field_name = field_name
        self.start = start
        self.end = end
        self.include_end = include_end

    def to_sql(self):
        inequality = '<'
        if self.include_end:
            inequality = '<='
        
        return f'({self.field_name} >= {self.start} and {self.field_name} {inequality} {self.end})'

class AndPredicate(Predicate):
    def __init__(self, predicates):
        self.predicates = predicates

    def to_sql(self):
        return ' and '.join([pred.to_sql() for pred in self.predicates])