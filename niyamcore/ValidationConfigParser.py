class ValidationConfigParser:
    """
    Parses dataset validation config JSON and provides an iterator
    to yield dataset name with each validation type and parameters.
    """

    def __init__(self, config_json):
        """
        Initialize with the JSON config as a dict.
        """
        self.dataset_name = config_json.get("dataset_name")
        self.validations = config_json.get("validations", {})

    def iter_validations(self):
        """
        Generator yielding tuples:
        (dataset_name, validation_type, validation_params)
        """
        for validation_type, params in self.validations.items():
            yield (self.dataset_name, validation_type, params)
