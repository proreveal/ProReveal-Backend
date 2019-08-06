from enum import Enum

class DataType(Enum):
    String = 'string'
    Integer = 'integer'
    Float = 'float'

    @staticmethod
    def from_string(string):
        if string == DataType.String.value:
            return DataType.String
        elif string == DataType.Integer.value:
            return DataType.Integer
        elif string == DataType.Float.value:
            return DataType.Float

        return None

class VlType(Enum):
    Quantitative = 'quantitative'
    Ordinal = 'ordinal'
    Nominal = 'nominal'
    Key = 'key'

    @staticmethod
    def from_string(string):
        if string == VlType.Quantitative.value:
            return VlType.Quantitative
        elif string == VlType.Ordinal.value:
            return VlType.Ordinal
        elif string == VlType.Nominal.value:
            return VlType.Nominal

        return VlType.Key

class FieldTrait:
    data_type = DataType.String
    vl_type = VlType.Quantitative
    
    def __init__(self, name, data_type):
        self.data_type = data_type
        self.name = name

    def to_json(self):
        return {'name': self.name, 'dataType': self.data_type.value, 'vlType': self.vl_type.value}

    def get_pyspark_sql_type(self):
        # from pyspark.sql.types import DoubleType, LongType, StringType
        if self.data_type is DataType.Integer:
            return LongType
        elif self.data_type is DataType.Float:
            return DoubleType
        elif self.data_type is DataType.String:
            return StringType

        return None

    @staticmethod
    def from_json(json):
        name = json['name']
        data_type = DataType.from_string(json['dataType'])
        vl_type = json['vlType']
        
        if vl_type == VlType.Quantitative.value:
            min = json.get('min', None)
            max = json.get('max', None)
            num_bins = json.get('numBins', None)

            return QuantitativeField(name, data_type, min, max, num_bins)
        elif vl_type == VlType.Ordinal.value:
            return OrdinalField(name, data_type)
        elif vl_type == VlType.Nominal.value:
            return NominalField(name, data_type)

        return KeyField(name, data_type)

class QuantitativeField(FieldTrait):
    vl_type = VlType.Quantitative

    def __init__(self, name, data_type, min, max, num_bins):
        super().__init__(name, data_type)

        self.min = min
        self.max = max
        self.num_bins = num_bins

    def to_json(self):
        return {'name': self.name, 'dataType': self.data_type.value, 
        'vlType': self.vl_type.value, 'min': self.min, 'max': self.max, 'num_bins': self.num_bins}

class CategoricalField(FieldTrait):
    pass

class OrdinalField(CategoricalField):
    vl_type = VlType.Ordinal

class NominalField(CategoricalField):
    vl_type = VlType.Nominal

class KeyField(CategoricalField):
    vl_type = VlType.Key
