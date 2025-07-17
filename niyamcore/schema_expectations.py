import logging
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructField, StructType,
    StringType, IntegerType, DoubleType, LongType, BooleanType, DateType, TimestampType,
    DecimalType # Added for common numeric types
)

# Initialize logger for this module
logger = logging.getLogger(__name__)

# A mapping from string data types in YAML to PySpark's DataType objects
# This makes it easier to compare types dynamically.
# Feel free to extend this mapping with more Spark types as needed.
PYSPARK_DATA_TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "decimal": DecimalType(), # Add parameters like precision, scale if needed, or parse from YAML
    # Add more types as your project requires, e.g., "array", "struct", "binary"
}

class SchemaValidator:
    """
    Validates a DataFrame's schema against a list of expected schema configurations.
    """

    def __init__(self, schema_expectations: list):
        """
        Initializes the SchemaValidator with a list of expected schema configurations.

        Args:
            schema_expectations (list): A list of dictionaries, where each dictionary
                                        describes an expected column (name, data_type, nullable).
                                        Example:
                                        [
                                            {"column_name": "product_id", "data_type": "string", "nullable": false},
                                            {"column_name": "stock_quantity", "data_type": "integer", "nullable": false}
                                        ]
        """
        if not isinstance(schema_expectations, list):
            raise TypeError("schema_expectations must be a list of dictionaries.")
        
        self.schema_expectations = schema_expectations
        logger.info(f"SchemaValidator initialized with {len(schema_expectations)} schema expectations.")

    def validate_schema(self, dataframe: DataFrame) -> list[dict]:
        """
        Validates the DataFrame's schema against the configured expectations.

        Args:
            dataframe (DataFrame): The Spark DataFrame whose schema is to be validated.

        Returns:
            list[dict]: A list of dictionaries, where each dictionary describes a
                        schema violation found. Returns an empty list if no violations.
                        Example error:
                        {
                            "type": "SchemaValidation",
                            "severity": "ERROR", # Schema errors are often critical
                            "message": "Column 'missing_col' is missing from DataFrame.",
                            "column_name": "missing_col",
                            "expected_type": "string",
                            "actual_type": None,
                            "expected_nullable": False,
                            "actual_nullable": None
                        }
        """
        schema_violations = []
        actual_schema_fields = {field.name: field for field in dataframe.schema}
        
        logger.info(f"Starting schema validation for DataFrame with {len(dataframe.schema)} columns.")

        for expected_col_spec in self.schema_expectations:
            col_name = expected_col_spec.get("column_name")
            expected_data_type_str = expected_col_spec.get("data_type")
            expected_nullable = expected_col_spec.get("nullable")

            if col_name is None:
                logger.warning("Schema expectation found without 'column_name'. Skipping.")
                continue

            if col_name not in actual_schema_fields:
                violation = {
                    "type": "SchemaValidation",
                    "severity": "ERROR",
                    "message": f"Column '{col_name}' is missing from DataFrame.",
                    "column_name": col_name,
                    "expected_type": expected_data_type_str,
                    "actual_type": None,
                    "expected_nullable": expected_nullable,
                    "actual_nullable": None
                }
                schema_violations.append(violation)
                logger.error(f"Schema violation: {violation['message']}")
                continue # No further checks for a missing column

            actual_field = actual_schema_fields[col_name]
            
            # 1. Validate Data Type
            if expected_data_type_str:
                expected_spark_type = PYSPARK_DATA_TYPE_MAP.get(expected_data_type_str.lower())
                if expected_spark_type is None:
                    logger.warning(f"Unknown expected data type '{expected_data_type_str}' for column '{col_name}'. Skipping type validation for this column.")
                elif actual_field.dataType != expected_spark_type:
                    violation = {
                        "type": "SchemaValidation",
                        "severity": "ERROR",
                        "message": f"Data type mismatch for column '{col_name}'. Expected '{expected_data_type_str}', got '{actual_field.dataType}'.",
                        "column_name": col_name,
                        "expected_type": expected_data_type_str,
                        "actual_type": str(actual_field.dataType), # Convert Spark type to string for reporting
                        "expected_nullable": expected_nullable,
                        "actual_nullable": actual_field.nullable
                    }
                    schema_violations.append(violation)
                    logger.error(f"Schema violation: {violation['message']}")

            # 2. Validate Nullability
            # Only check if expected_nullable is explicitly defined in the config
            if expected_nullable is not None and actual_field.nullable != expected_nullable:
                violation = {
                    "type": "SchemaValidation",
                    "severity": "ERROR",
                    "message": f"Nullability mismatch for column '{col_name}'. Expected nullable: {expected_nullable}, actual nullable: {actual_field.nullable}.",
                    "column_name": col_name,
                    "expected_type": expected_data_type_str,
                    "actual_type": str(actual_field.dataType),
                    "expected_nullable": expected_nullable,
                    "actual_nullable": actual_field.nullable
                }
                schema_violations.append(violation)
                logger.error(f"Schema violation: {violation['message']}")
        
        if not schema_violations:
            logger.info("Schema validation completed: No violations found.")
        else:
            logger.warning(f"Schema validation completed: Found {len(schema_violations)} violations.")
            
        return schema_violations