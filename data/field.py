from enum import Enum
from pyspark.sql.types import DoubleType, IntegerType, StringType

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
        return {'name': self.name, 'dataType': self.data_type, 'vlType': self.vl_type}

    def get_pyspark_sql_type(self):
        if self.data_type is DataType.Integer:
            return IntegerType
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
        
        if vl_type == VlType.Quantitative:
            return QuantitativeField(name, data_type)
        elif vl_type == VlType.Ordinal:
            return OrdinalField(name, data_type)
        elif vl_type == VlType.Nominal:
            return NominalField(name, data_type)

        return KeyField(name, data_type)

class QuantitativeField(FieldTrait):
    vl_type = VlType.Quantitative

class CategoricalField(FieldTrait):
    pass

class OrdinalField(CategoricalField):
    vl_type = VlType.Ordinal

class NominalField(CategoricalField):
    vl_type = VlType.Nominal

class KeyField(CategoricalField):
    vl_type = VlType.Key
