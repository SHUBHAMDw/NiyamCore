# niyamcore/ColumnValidator.py

import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
from niyamcore.rules.rule_factory import create_rule

logger = logging.getLogger(__name__)

class ColumnValidator:
    """
    Orchestrates the application of column-level validation rules.
    """

    def __init__(self, column_validations_config: list, error_column_name: str = "validation_errors"):
        """
        Initializes the ColumnValidator.

        Args:
            column_validations_config (list): A list of dictionaries, where each dict
                                              describes a column to validate and its rules.
                                              Example:
                                              [
                                                {"column": "product_id", "rules": [...]},
                                                {"column": "stock_quantity", "rules": [...]}
                                              ]
            error_column_name (str): The name of the column where error messages will be accumulated.
        """
        if not isinstance(column_validations_config, list):
            raise TypeError("column_validations_config must be a list of dictionaries.")

        self.column_validations_config = column_validations_config
        self.error_column_name = error_column_name
        logger.info(f"ColumnValidator initialized with {len(column_validations_config)} column validation groups.")

    def apply_validations(self, dataframe: DataFrame) -> DataFrame:
        """
        Applies all configured column-level validations to the DataFrame.
        Initializes an error column and accumulates error messages for each row.

        Args:
            dataframe (DataFrame): The input Spark DataFrame.

        Returns:
            DataFrame: The DataFrame with the accumulated error messages in the error_column_name.
        """
        if not self.column_validations_config:
            logger.info("No column validations configured. Returning DataFrame unchanged.")
            return dataframe

        logger.info(f"Starting column validations. Error column: '{self.error_column_name}'.")

        # Initialize the error column if it doesn't exist or ensure it's the correct type.
        if self.error_column_name not in dataframe.columns:
            dataframe = dataframe.withColumn(self.error_column_name, F.array().cast(ArrayType(StringType())))
            logger.debug(f"Initialized error column '{self.error_column_name}' in DataFrame.")
        else:
            # If it exists, ensure it's an ArrayType(StringType()). Attempt to cast if not.
            if dataframe.schema[self.error_column_name].dataType != ArrayType(StringType()):
                 logger.warning(f"Existing error column '{self.error_column_name}' is not of type ArrayType(StringType()). Attempting to cast.")
                 dataframe = dataframe.withColumn(self.error_column_name, F.col(self.error_column_name).cast(ArrayType(StringType())))
            # Also, ensure existing values are arrays, not nulls or other types that could break array_union
            dataframe = dataframe.withColumn(
                self.error_column_name,
                F.when(F.col(self.error_column_name).isNull(), F.array().cast(ArrayType(StringType())))
                .otherwise(F.col(self.error_column_name))
            )


        processed_df = dataframe

        for col_val_group in self.column_validations_config:
            column_name = col_val_group.get("column")
            rules_config = col_val_group.get("rules", [])

            if not column_name:
                logger.warning("Column validation group found without 'column_name'. Skipping.")
                continue
            if not rules_config:
                logger.warning(f"Column '{column_name}' has no rules defined. Skipping.")
                continue
            # Note: We do not check if column_name exists here to allow rules like "NotNullRule"
            # to be applied to a column that might be conditionally missing (though SchemaValidator
            # should catch this first). The rule's validate method will handle missing columns.

            logger.debug(f"Applying rules for column: '{column_name}' ({len(rules_config)} rules)")

            for rule_conf in rules_config:
                try:
                    rule_instance = create_rule(rule_conf)
                    processed_df = rule_instance.validate(processed_df, column_name, self.error_column_name)
                    logger.debug(f"Applied rule '{rule_instance.__class__.__name__}' to '{column_name}'.")

                except ValueError as ve:
                    logger.error(f"Configuration error for rule in column '{column_name}': {ve}. Rule config: {rule_conf}")
                    # You might want to add a generic error to the DataFrame here if the rule config is invalid
                except Exception as e:
                    logger.error(f"Unexpected error applying rule '{rule_conf.get('type')}' to column '{column_name}': {e}. Rule config: {rule_conf}")
                    # Add a generic error message to the DataFrame for unexpected rule failures
                    processed_df = processed_df.withColumn(
                        self.error_column_name,
                        F.array_union(
                            F.col(self.error_column_name).cast("array<string>"),
                            F.array(F.lit(f"[CRITICAL_ERROR] Failed to apply rule '{rule_conf.get('type')}' to '{column_name}': {e}"))
                        )
                    )

        logger.info("Column validations completed.")
        return processed_df