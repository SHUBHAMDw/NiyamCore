from niyamcore.base_validations import NiyamCoreValidator

# Usage example
# validator = NiyamCoreValidator(df)
# result = validator.not_null_check(['customer_id', 'email'])
# print(result)

class run_validation:
    def run_validation(config_dict):
        print("Hello guysssss!")
        for validation_tile in config_dict["validation_config"].keys():
            print(validation_tile)
            if validation_tile == "schema_expectations":