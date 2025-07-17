# niyamcore/rules/base_rule.py

import logging
from abc import ABC, abstractmethod
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

class BaseValidationRule(ABC):
    """
    Abstract Base Class for all data validation rules.
    Defines the common interface for how rules are initialized and applied.
    """

    def __init__(self,
                 severity: str,
                 error_message: str,
                 when_condition: Optional[str] = None,
                 **kwargs):
        """
        Initializes the base validation rule.

        Args:
            severity (str): The severity of the validation failure (e.g., "ERROR", "WARNING").
            error_message (str): The message to append to the error column if the rule fails.
            when_condition (str, optional): A Spark SQL expression string. If provided, the rule
                                            only applies when this condition is true for a row.
            **kwargs: Rule-specific parameters.
        """
        self.severity = severity
        self.error_message = error_message
        self.when_condition = when_condition
        self.rule_params = kwargs # Store any additional rule-specific parameters

        if self.when_condition:
            logger.debug(f"Rule initialized with when_condition: '{self.when_condition}'")

    @abstractmethod
    def _get_failure_condition(self, column_name: str) -> Column:
        """
        Abstract method to be implemented by concrete rule classes.
        Returns a Spark SQL Column expression that evaluates to TRUE if the validation FAILS
        for the given column, and FALSE otherwise. This expression should NOT consider
        the 'when_condition'.
        """
        pass

    def _apply_when_condition(self, failure_condition: Column) -> Column:
        """
        Applies the 'when_condition' to the basic failure condition.
        Returns a Spark SQL Column expression that is TRUE if the rule applies AND FAILS.
        """
        if self.when_condition:
            try:
                when_expr = F.expr(self.when_condition)
                return when_expr & failure_condition
            except Exception as e:
                logger.error(f"Invalid 'when_condition' expression '{self.when_condition}': {e}. This rule might not apply correctly.")
                # If when_condition is invalid, it's safer to not apply the rule or
                # to mark the condition itself as a failure if it's critical.
                # For this implementation, we will return a literal False if the expression is invalid
                # to prevent unexpected behavior, meaning the rule won't fire for rows even if they fail.
                # A more robust approach might add an error specifically for the invalid condition.
                return F.lit(False)
        return failure_condition

    def validate(self, dataframe: DataFrame, column_name: str, error_column_name: str) -> DataFrame:
        """
        Applies the validation rule to the specified column of the DataFrame.
        Adds the error message to the error_column_name if the rule fails.

        Args:
            dataframe (DataFrame): The input Spark DataFrame.
            column_name (str): The name of the column to validate.
            error_column_name (str): The name of the column where errors are accumulated.

        Returns:
            DataFrame: The DataFrame with potential updates to the error column.
        """
        if column_name not in dataframe.columns:
            logger.warning(f"Column '{column_name}' not found in DataFrame for rule '{self.__class__.__name__}'. Skipping validation.")
            # If the column is missing, the rule can't apply to it.
            # You might consider adding a meta-error about the missing column
            # if that's a critical configuration issue for your framework.
            return dataframe

        # Get the base failure condition (e.g., column is null)
        base_failure_condition = self._get_failure_condition(column_name)

        # Apply the when_condition if present
        final_failure_condition = self._apply_when_condition(base_failure_condition)

        # Update the error column:
        # If final_failure_condition is TRUE, union the current error message
        # with the existing errors in the error_column.
        # Ensure the error_column exists and is an array of strings.

        return dataframe.withColumn(
            error_column_name,
            F.when(
                final_failure_condition,
                F.array_union(
                    F.col(error_column_name).cast("array<string>"), # Cast to handle potential nulls or incorrect types
                    F.array(F.lit(f"[{self.severity}] {self.error_message}")) # Prepend severity to message
                )
            ).otherwise(F.col(error_column_name))
        )