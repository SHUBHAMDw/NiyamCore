# niyamcore/RowValidator.py

import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType

logger = logging.getLogger(__name__)

class RowValidator:
    """
    Validates rows in a DataFrame based on custom conditions that can involve
    multiple columns within a single row.
    """

    def __init__(self, row_validations_config: list, error_column_name: str = "validation_errors"):
        """
        Initializes the RowValidator.

        Args:
            row_validations_config (list): A list of dictionaries, where each dict
                                           defines a row-level validation rule.
                                           Example:
                                           [
                                               {"name": "PositiveStock", "condition": "stock_quantity <= 0 AND product_status = 'ACTIVE'", "severity": "ERROR", "error_message": "Active products must have positive stock."},
                                               {"name": "DateRange", "condition": "start_date > end_date", "severity": "WARNING", "error_message": "Start date cannot be after end date."}
                                           ]
            error_column_name (str): The name of the column where error messages will be accumulated.
        """
        if not isinstance(row_validations_config, list):
            raise TypeError("row_validations_config must be a list of dictionaries.")
        
        self.row_validations_config = row_validations_config
        self.error_column_name = error_column_name
        logger.info(f"RowValidator initialized with {len(row_validations_config)} row validation rules.")

    def apply_validations(self, dataframe: DataFrame) -> DataFrame:
        """
        Applies all configured row-level validations to the DataFrame.
        Adds error messages to the specified error column for rows that fail.

        Args:
            dataframe (DataFrame): The input Spark DataFrame.

        Returns:
            DataFrame: The DataFrame with accumulated row-level error messages.
        """
        if not self.row_validations_config:
            logger.info("No row validations configured. Returning DataFrame unchanged.")
            return dataframe

        logger.info(f"Starting row validations. Error column: '{self.error_column_name}'.")

        # Ensure the error column exists and is of the correct type (ArrayType(StringType))
        # This duplicates logic from ColumnValidator, but it ensures robustness if RowValidator
        # is called independently or if the error column isn't pre-initialized by ColumnValidator.
        if self.error_column_name not in dataframe.columns:
            dataframe = dataframe.withColumn(self.error_column_name, F.array().cast(ArrayType(StringType())))
            logger.debug(f"Initialized error column '{self.error_column_name}' in DataFrame for row validations.")
        else:
            if dataframe.schema[self.error_column_name].dataType != ArrayType(StringType()):
                 logger.warning(f"Existing error column '{self.error_column_name}' is not of type ArrayType(StringType()). Attempting to cast for row validations.")
                 dataframe = dataframe.withColumn(self.error_column_name, F.col(self.error_column_name).cast(ArrayType(StringType())))
            # Ensure existing values are arrays, not nulls or other types that could break array_union
            dataframe = dataframe.withColumn(
                self.error_column_name,
                F.when(F.col(self.error_column_name).isNull(), F.array().cast(ArrayType(StringType())))
                .otherwise(F.col(self.error_column_name))
            )

        processed_df = dataframe

        for row_rule_config in self.row_validations_config:
            rule_name = row_rule_config.get("name", "Unnamed_Row_Rule")
            condition_str = row_rule_config.get("condition")
            severity = row_rule_config.get("severity", "ERROR") # Default to ERROR
            error_message = row_rule_config.get("error_message")

            if not condition_str:
                logger.warning(f"Row validation rule '{rule_name}' is missing 'condition'. Skipping.")
                continue
            if not error_message:
                logger.warning(f"Row validation rule '{rule_name}' is missing 'error_message'. Using default.")
                error_message = f"Validation failed for row rule: {rule_name}."

            logger.debug(f"Applying row rule '{rule_name}' with condition: '{condition_str}'")

            try:
                # Parse the condition string into a Spark SQL Column expression
                failure_condition_expr = F.expr(condition_str)

                # Update the error column for rows where the condition is true (i.e., validation failed)
                processed_df = processed_df.withColumn(
                    self.error_column_name,
                    F.when(
                        failure_condition_expr,
                        F.array_union(
                            F.col(self.error_column_name).cast("array<string>"),
                            F.array(F.lit(f"[{severity}] {error_message}"))
                        )
                    ).otherwise(F.col(self.error_column_name))
                )
                logger.debug(f"Applied row rule '{rule_name}'.")

            except Exception as e:
                logger.error(f"Error applying row validation rule '{rule_name}' with condition '{condition_str}': {e}. This rule was skipped.")
                # Add a generic error to the DataFrame if the rule itself failed to apply due to malformed condition
                processed_df = processed_df.withColumn(
                    self.error_column_name,
                    F.array_union(
                        F.col(self.error_column_name).cast("array<string>"),
                        F.array(F.lit(f"[CRITICAL_ERROR] Failed to apply row rule '{rule_name}': {e}"))
                    )
                )

        logger.info("Row validations completed.")
        return processed_df