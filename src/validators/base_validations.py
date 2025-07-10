from pyspark.sql.functions import col, max as spark_max
from datetime import datetime

class NiyamCoreValidator:
    """
    A data quality validator class with configurable validation methods
    to enforce data quality rules on PySpark DataFrames.
    """

    def __init__(self, df):
        """
        Initialize with a Spark DataFrame to validate.
        """
        self.df = df

    def not_null_check(self, columns):
        """
        Validate that specified columns contain no null values.
        Returns dict with results per column.
        """
        result = {}
        for c in columns:
            null_count = self.df.filter(col(c).isNull()).count()
            result[c] = {
                "passed": null_count == 0,
                "null_count": null_count
            }
        return result

    def unique_check(self, columns):
        """
        Check if values in specified columns are unique.
        Returns dict with duplicate counts per column.
        """
        result = {}
        total = self.df.count()
        for c in columns:
            distinct = self.df.select(c).distinct().count()
            result[c] = {
                "passed": total == distinct,
                "duplicates": total - distinct
            }
        return result

    def regex_check(self, column, pattern):
        """
        Validate that all values in the column match the given regex pattern.
        Returns dict with count of invalid values.
        """
        invalid_count = self.df.filter(~col(column).rlike(pattern)).count()
        return {
            "passed": invalid_count == 0,
            "invalid_count": invalid_count
        }

    def range_check(self, column, min_val, max_val):
        """
        Check if values in the column fall within the numeric range [min_val, max_val].
        Returns count of out-of-range values.
        """
        out_of_range = self.df.filter((col(column) < min_val) | (col(column) > max_val)).count()
        return {
            "passed": out_of_range == 0,
            "out_of_range": out_of_range
        }

    def value_set_check(self, column, valid_values):
        """
        Verify that all values in the column belong to a predefined set of valid values.
        Returns count of invalid values.
        """
        invalid_count = self.df.filter(~col(column).isin(valid_values)).count()
        return {
            "passed": invalid_count == 0,
            "invalid_count": invalid_count
        }

    def data_type_check(self, column, expected_type):
        """
        Check if the column's data type matches the expected Spark SQL type.
        """
        actual_type = dict(self.df.dtypes).get(column)
        passed = actual_type == expected_type
        return {
            "passed": passed,
            "expected_type": expected_type,
            "actual_type": actual_type
        }

    def freshness_check(self, timestamp_column, max_delay_hours):
        """
        Validate that the latest timestamp in the timestamp_column
        is within max_delay_hours from the current time.
        """
        max_timestamp = self.df.agg(spark_max(timestamp_column)).collect()[0][0]
        if max_timestamp is None:
            return {
                "passed": False,
                "message": "No timestamp data found"
            }
        current_ts = datetime.now()
        delay_hours = (current_ts - max_timestamp).total_seconds() / 3600
        return {
            "passed": delay_hours <= max_delay_hours,
            "delay_hours": delay_hours
        }

    def completeness_check(self, expected_count):
        """
        Check if the DataFrame contains at least the expected number of rows.
        """
        actual_count = self.df.count()
        passed = actual_count >= expected_count
        return {
            "passed": passed,
            "expected_count": expected_count,
            "actual_count": actual_count
        }

    def duplicate_row_check(self, columns):
        """
        Detect duplicate rows based on specified columns.
        Returns number of duplicate rows found.
        """
        total = self.df.count()
        distinct = self.df.dropDuplicates(columns).count()
        duplicates = total - distinct
        return {
            "passed": duplicates == 0,
            "duplicate_count": duplicates
        }

    @staticmethod
    def referential_integrity_check(df_child, child_column, df_parent, parent_column):
        """
        Verify that all values in child_column of df_child exist in parent_column of df_parent.
        Returns count of invalid (orphan) values.
        """
        invalid_count = df_child.join(
            df_parent,
            df_child[child_column] == df_parent[parent_column],
            "left_anti"
        ).count()
        return {
            "passed": invalid_count == 0,
            "invalid_count": invalid_count
        }

    def custom_rule_check(self, rule_fn):
        """
        Apply a custom validation rule defined by rule_fn.
        rule_fn should take a DataFrame and return the count of violating rows.
        """
        invalid_count = rule_fn(self.df)
        return {
            "passed": invalid_count == 0,
            "invalid_count": invalid_count
        }
