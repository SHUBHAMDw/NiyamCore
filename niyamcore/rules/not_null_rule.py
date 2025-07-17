# niyamcore/rules/not_null_rule.py

from pyspark.sql.column import Column
import pyspark.sql.functions as F
from niyamcore.rules.base_rule import BaseValidationRule

class NotNullRule(BaseValidationRule):
    """
    Validation rule to check if a column's values are not null.
    """
    def __init__(self, severity: str, error_message: str, when_condition: str = None, **kwargs):
        super().__init__(severity, error_message, when_condition, **kwargs)

    def _get_failure_condition(self, column_name: str) -> Column:
        """
        Returns a Spark SQL expression that is TRUE if the column is NULL.
        """
        return F.col(column_name).isNull()