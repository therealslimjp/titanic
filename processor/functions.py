from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType, BooleanType
from pyspark.sql import DataFrame
from  pyspark.sql.functions import lit


def json_to_schema(json_obj):
    """Convert a JSON object to a Spark StructType schema."""
    columns = set()
    for key, val in json_obj.items():
        if isinstance(val, bool):
            columns.add(StructField(key, BooleanType(), True))
        elif isinstance(val, int):
            columns.add(StructField(key, IntegerType(), True))
        elif isinstance(val, float):
            columns.add(StructField(key, DoubleType(), True))
        elif isinstance(val, str):
            columns.add(StructField(key, StringType(), True))
        elif isinstance(val, list):
            columns.add(StructField(key, ArrayType(StringType()), True))
        else:
            columns.add(StructField(key, StringType(), True))
    return StructType(list(columns))


def merge_schemas(schema1, schema2):
    """Merge two StructType schemas into one."""
    # Convert schemas to field maps for easy lookup
    fields1 = {field.name: field for field in schema1.fields}
    fields2 = {field.name: field for field in schema2.fields}

    # Merge the field maps
    merged_fields = fields1.copy()
    for name, field in fields2.items():
        if name not in merged_fields:
            merged_fields[name] = field
        else:
            # Example conflict resolution: check if types are the same; raise error if not
            if field.dataType != merged_fields[name].dataType:
                raise ValueError(f"Conflict in field '{name}': {field.dataType} vs {merged_fields[name].dataType}")

    # Create a new StructType from the merged field map
    merged_schema = StructType(list(merged_fields.values()))
    return merged_schema


def make_df_and_target_table_schema_compatible(df: DataFrame, table_schema: StructType):
    # Get a list of column names in the order they appear in the table schema
    table_columns_ordered = [field.name for field in table_schema.fields]
    # Identify DataFrame columns not present in the table schema
    extra_columns = [column for column in df.columns if column not in table_columns_ordered]
    # Identify columns present in the table schema but missing from the DataFrame
    missing_columns = [column for column in table_columns_ordered if column not in df.columns]
    # For each missing column, add it to the DataFrame with null values
    for col in missing_columns:
        df = df.withColumn(col, lit(None))
    # Reorder DataFrame columns to match the table schema order, including the newly added null columns
    all_cols=[column.name for column in table_schema.fields]+extra_columns
    df_ordered = df.select(all_cols)
    return df_ordered, extra_columns

