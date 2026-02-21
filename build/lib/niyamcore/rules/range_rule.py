# niyamcore/rules/range_rule.py

from typing import Union
from pyspark.sql.column import Column
import pyspark.sql.functions as F
from niyamcore.rules.base_rule import BaseValidationRule

class RangeRule(BaseValidationRule):
    """
    Validation rule to check if a column's values are within a specified min/max range (inclusive).
    """
    def __init__(self, severity: str, error_message: str, min: Union[int, float], max: Union[int, float], when_condition: str = None, **kwargs):
        super().__init__(severity, error_message, when_condition, **kwargs)
        
        if not isinstance(min, (int, float)) or not isinstance(max, (int, float)):
            raise TypeError("Min and Max values for RangeRule must be numeric.")
        if min > max:
            raise ValueError("Min value cannot be greater than Max value for RangeRule.")

        self.min_val = min
        self.max_val = max

    def _get_failure_condition(self, column_name: str) -> Column:
        """
        Returns a Spark SQL expression that is TRUE if the column's value is
        outside the specified [min_val, max_val] range.
        Handles nulls gracefully (nulls are not considered within range unless explicitly handled).
        """
        # A value fails if it's not null AND (less than min OR greater than max)
        return F.col(column_name).isNotNull() & \
               ((F.col(column_name) < self.min_val) | (F.col(column_name) > self.max_val))