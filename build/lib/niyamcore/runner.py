# runner.py

import logging
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType

# Import the validator classes
from niyamcore.schema_expectations import SchemaValidator
from niyamcore.ColumnValidator import ColumnValidator
from niyamcore.RowValidator import RowValidator

logger = logging.getLogger(__name__)

class ValidationRunner:
    """
    Orchestrates the entire data validation pipeline.
    It takes the parsed configuration and a DataFrame, then applies schema,
    column, and row validations. It also manages the output based on settings.
    """
    def Test():
        print("Test method in ValidationRunner called.")

    def __init__(self, spark_session: SparkSession):
        """
        Initializes the ValidationRunner.

        Args:
            spark_session (SparkSession): The active SparkSession.
        """
        self.spark = spark_session
        logger.info("ValidationRunner initialized.")

    def run_validations(self, dataframe: DataFrame, parsed_config: dict) -> DataFrame:
        """
        Executes the full validation pipeline on the given DataFrame.

        Args:
            dataframe (DataFrame): The input Spark DataFrame to validate.
            parsed_config (dict): The full parsed YAML configuration dictionary.

        Returns:
            DataFrame: The validated DataFrame, potentially with an added
                       error column, or an empty DataFrame if 'fail_dataframe' mode
                       and critical errors are found.

        Raises:
            Exception: If 'fail_dataframe' mode is enabled and critical errors are found.
        """
        logger.info("Starting validation pipeline...")

        # --- 1. Get Global Validation Settings ---
        validation_settings = parsed_config.get("validation_settings", {})
        output_mode = validation_settings.get("output_mode", "add_error_column")
        error_column_name = validation_settings.get("error_column_name", "validation_errors")
        fail_on_severity = validation_settings.get("fail_on_severity", "ERROR").upper() # Default to fail on ERROR

        # Initialize a global error column at the start, ensuring it's of the correct type
        # This makes sure all validators append to a consistent column.
        if error_column_name not in dataframe.columns:
            dataframe = dataframe.withColumn(error_column_name, F.array().cast(ArrayType(StringType())))
        else:
            # Ensure it's the correct type if it already exists, or cast it.
            if dataframe.schema[error_column_name].dataType != ArrayType(StringType()):
                 logger.warning(f"Existing error column '{error_column_name}' is not of type ArrayType(StringType()). Attempting to cast.")
                 dataframe = dataframe.withColumn(error_column_name, F.col(error_column_name).cast(ArrayType(StringType())))
            # Also, ensure existing values are arrays, not nulls or other types that could break array_union
            dataframe = dataframe.withColumn(
                error_column_name,
                F.when(F.col(error_column_name).isNull(), F.array().cast(ArrayType(StringType())))
                .otherwise(F.col(error_column_name))
            )

        validated_df = dataframe
        all_schema_errors_found = []

        # --- 2. Run Schema Validations ---
        schema_expectations = parsed_config.get("schema_expectations")
        if schema_expectations:
            logger.info("Running Schema Validations...")
            schema_validator = SchemaValidator(schema_expectations)
            current_schema_errors = schema_validator.validate_schema(validated_df)
            
            if current_schema_errors:
                all_schema_errors_found.extend(current_schema_errors)
                logger.warning(f"Found {len(current_schema_errors)} schema violations.")
                # For schema errors, we don't add to row-level error column usually,
                # as they are structural. We log/report them separately.
                # If a critical column is missing due to schema, subsequent validations might fail.
            else:
                logger.info("Schema Validations passed.")

        # --- Check if any critical schema errors demand immediate halt (if fail_dataframe is on) ---
        if output_mode == "fail_dataframe":
            critical_schema_errors = [e for e in all_schema_errors_found if e.get('severity', 'ERROR').upper() == fail_on_severity]
            if critical_schema_errors:
                error_messages = "\n".join([e.get('message', 'Unknown schema error') for e in critical_schema_errors])
                logger.error(f"Critical schema errors found. Halting validation: \n{error_messages}")
                # You might return an empty DF or raise a more specific exception
                raise Exception(f"Validation failed due to critical schema errors: {error_messages}")
                # Or, if you want to return an empty DF:
                # return self.spark.createDataFrame([], validated_df.schema)


        # --- 3. Run Column Validations ---
        column_validations = parsed_config.get("column_validations")
        if column_validations:
            logger.info("Running Column Validations...")
            column_validator = ColumnValidator(column_validations, error_column_name)
            validated_df = column_validator.apply_validations(validated_df)
            logger.info("Column Validations applied.")

        # --- 4. Run Row Validations ---
        row_validations = parsed_config.get("row_validations")
        if row_validations:
            logger.info("Running Row Validations...")
            row_validator = RowValidator(row_validations, error_column_name)
            validated_df = row_validator.apply_validations(validated_df)
            logger.info("Row Validations applied.")

        # --- 5. Final Output Mode Handling ---
        total_rows_with_errors = validated_df.filter(F.size(F.col(error_column_name)) > 0).count()
        logger.info(f"Validation pipeline completed. {total_rows_with_errors} rows contain errors.")

        # If schema errors were found, they are reported here.
        if all_schema_errors_found:
            logger.warning(f"Summary of Schema Violations ({len(all_schema_errors_found)} found):")
            for error in all_schema_errors_found:
                logger.warning(f"  [SCHEMA] [{error.get('severity')}] {error.get('message')} (Column: {error.get('column_name')})")

        # Determine if any critical row/column errors occurred based on fail_on_severity
        has_critical_row_column_errors = False
        if total_rows_with_errors > 0:
            # We need to collect errors (can be expensive for large DFs) or just check for existence of severe ones.
            # A more performant way is to check the severity within the Spark expression itself.
            # For simplicity, we'll check if ANY error in the array has the fail_on_severity keyword.
            
            # This expression checks if any string in the array starts with "[FAIL_ON_SEVERITY]"
            # It's an approximation. A more robust solution might require UDFs or splitting the string.
            critical_error_regex = f"^\\[{fail_on_severity}\\]"
            
            critical_error_rows_count = validated_df.filter(
                F.array_contains(
                    F.col(error_column_name),
                    F.lit(f"[{fail_on_severity}]") # Check for exact string matching if messages are standardized
                ) | F.exists(F.col(error_column_name), lambda x: x.rlike(critical_error_regex)) # More flexible check
            ).count()

            if critical_error_rows_count > 0:
                has_critical_row_column_errors = True
                logger.error(f"Found {critical_error_rows_count} rows with errors of severity '{fail_on_severity}' or higher.")

        if output_mode == "fail_dataframe" and has_critical_row_column_errors:
            logger.error(f"Validation failed due to critical errors and 'fail_dataframe' output mode. Halting.")
            # Depending on desired behavior, you could return an empty DataFrame or raise an exception.
            # Raising an exception is generally clearer for pipeline failures.
            raise Exception(f"Validation failed: {critical_error_rows_count} rows contain errors of severity '{fail_on_severity}' or higher.")
            # return self.spark.createDataFrame([], validated_df.schema) # Alternative: return empty DF

        elif output_mode == "log_only":
            logger.info("Output mode is 'log_only'. Validation errors are logged above, DataFrame returned without explicit error column (if configured).")
            # If log_only means no error column in final output, you'd drop it here.
            # For now, we'll return it with the error column for visibility during debugging.
            # If you truly want to drop it:
            # return validated_df.drop(error_column_name)

        elif output_mode == "add_error_column":
            logger.info(f"Output mode is 'add_error_column'. DataFrame returned with '{error_column_name}' containing errors.")
        
        else:
            logger.warning(f"Unknown output_mode: '{output_mode}'. Defaulting to 'add_error_column' behavior.")

        return validated_df