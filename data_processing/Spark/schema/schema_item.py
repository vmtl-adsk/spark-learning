from pyspark.sql.types import FloatType, StringType, StructField, StructType, LongType


def item_schema():
    return StructType([
        StructField('item_id', LongType()),
        StructField('item_name', StringType()),
        StructField('description', StringType()),
        StructField('price', FloatType()),
        StructField('tax', FloatType()),
        StructField('tax_with_price', FloatType())
    ])
