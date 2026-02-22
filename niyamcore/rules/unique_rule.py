# niyamcore/rules/unique_rule.py

import logging
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
import pyspark.sql.functions as F
from pyspark.sql.window import Window # Needed for advanced unique checks if done differently
from niyamcore.rules.base_rule import BaseValidationRule

logger = logging.getLogger(__name__)

class UniqueRule(BaseValidationRule):
    """
    Validation rule to check if a column's values are unique within the dataset.
    This rule operates on the DataFrame level to identify duplicates
    and adds error messages to *each* duplicate row.
    """
    def __init__(self, severity: str, error_message: str, when_condition: str = None, **kwargs):
        super().__init__(severity, error_message, when_condition, **kwargs)
        # Note: 'when_condition' for UniqueRule is applied to filter the dataset
        # before finding duplicates, ensuring the uniqueness check is context-aware.

    def _get_failure_condition(self, column_name: str) -> Column:
        """
        This method is not directly used for the unique rule's main logic
        because uniqueness is an aggregate/window check, not a simple row-wise expression.
        We override the 'validate' method instead.
        """
        raise NotImplementedError("UniqueRule implements custom DataFrame transformation logic; _get_failure_condition is not applicable.")

    def validate(self, dataframe: DataFrame, column_name: str, error_column_name: str) -> DataFrame:
        """
        Applies the unique validation rule to the specified column.
        Identifies duplicate values (considering 'when_condition' if present) and adds
        error messages to all rows that are part of a duplicate set.
        """
        if column_name not in dataframe.columns:
            logger.warning(f"Column '{column_name}' not found in DataFrame for UniqueRule. Skipping validation.")
            return dataframe

        logger.debug(f"Applying UniqueRule for column '{column_name}' with severity '{self.severity}'.")

        # Create a temporary DataFrame to find duplicate values.
        # The uniqueness check should only apply to non-null values.
        # If 'when_condition' is specified, we first filter the DataFrame to that condition
        # to find duplicates ONLY within that subgroup.
        df_for_uniqueness_check = dataframe
        if self.when_condition:
            try:
                df_for_uniqueness_check = dataframe.filter(F.expr(self.when_condition))
                logger.debug(f"UniqueRule for column '{column_name}' applying 'when_condition': {self.when_condition} to find duplicates.")
            except Exception as e:
                logger.error(f"Invalid 'when_condition' expression '{self.when_condition}' for UniqueRule: {e}. Skipping unique validation.")
                return dataframe # Return original DF if condition is invalid

        # Find duplicate values in the relevant subset of the data
        # We only consider non-null values for uniqueness check
        duplicate_values_df = df_for_uniqueness_check.groupBy(column_name) \
                                                    .count() \
                                                    .filter(F.col("count") > 1) \
                                                    .filter(F.col(column_name).isNotNull()) \
                                                    .select(column_name)
        print("Duplicate values found for UniqueRule on column '{}':".format(column_name)) # Debugging line
        duplicate_values_df.show(5) # Debugging line to check the duplicate values found--can be removed in production

        # Broadcast the smaller DataFrame of duplicate values for efficient joining
        # Mark rows in the original DataFrame that contain a duplicate value
        df_with_duplicates_flagged = dataframe.join(
            F.broadcast(duplicate_values_df),
            on=[column_name],
            how="left_outer"
        ).withColumn(
            "_is_duplicate_value",
            F.col(duplicate_values_df.columns[0]).isNotNull() # If joined column is not null, it's a duplicate value
        )

        # The final condition for adding an error message to a row:
        # The row's value is a duplicate AND (if a when_condition exists, that condition is also true for this row)
        # This means, if a row fails the uniqueness check, it will get the error.
        # If there's a when_condition, and the row doesn't meet the when_condition, it won't get the error,
        # even if its value would be considered a duplicate outside of the condition's scope.
        final_failure_condition = df_with_duplicates_flagged["_is_duplicate_value"]
        
        # If a when_condition was applied to *filter* the check, we need to apply it again
        # on the original dataframe to correctly mark the error messages.
        if self.when_condition:
            final_failure_condition = F.expr(self.when_condition) & final_failure_condition

        # Update the error column
        result_df = df_with_duplicates_flagged.withColumn(
            error_column_name,
            F.when(
                final_failure_condition,
                F.array_union(
                    F.col(error_column_name).cast("array<string>"),
                    F.array(F.lit(f"[{self.severity}] {self.error_message}")) # Prepend severity
                )
            ).otherwise(F.col(error_column_name))
        ).drop("_is_duplicate_value") # Clean up the temporary flag column

        logger.debug(f"Finished UniqueRule for column '{column_name}'.")
        return result_df