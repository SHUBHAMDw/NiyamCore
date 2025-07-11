from validators.base_validations import NiyamCoreValidator

# Usage example
validator = NiyamCoreValidator(df)
result = validator.not_null_check(['customer_id', 'email'])
print(result)
