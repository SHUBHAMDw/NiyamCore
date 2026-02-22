import yaml
import logging
from niyamcore.runner import ValidationRunner
from pyspark.sql import DataFrame, SparkSession
# from niyamcore.runner import run_validation
logger = logging.getLogger(__name__) 
class ValidationConfigParser:
    """
    Parses dataset validation config JSON and provides an iterator
    to yield dataset name with each validation type and parameters.
    """
    logger = logging.getLogger(__name__) 

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

    @staticmethod
    def parse_config(spark,dataframe: DataFrame,config_path: str) -> dict:
        """
        Loads and parses a YAML configuration file from the given path.
        Assumes the path is accessible by the Databricks cluster (e.g., DBFS, Volumes, Workspace Files).

        Args:
            config_path: The full path to the YAML configuration file (e.g., "dbfs:/FileStore/my_rules.yaml").

        Returns:
            A dictionary representation of the parsed YAML config.

        Raises:
            FileNotFoundError: If the config file does not exist or is empty.
            yaml.YAMLError: If there's an error parsing the YAML content.
            Exception: For other unexpected errors during file reading.
        """
        # spark = SparkSession.builder.getOrCreate()
        config_content = ""

        try:
            # Attempt to read the file content using Spark's text reader.
            # This is robust for various file systems supported by Spark (dbfs:/, /Volumes/, /Workspace/)
            config_df = spark.read.text(config_path)
            
            # Collect all lines and join them.
            # .collect() brings data to driver, so be mindful of very large config files (> ~10MB)
            config_content = "\n".join([row[0] for row in config_df.collect()])
            
            if not config_content.strip(): # Check if content is empty after stripping whitespace
                raise FileNotFoundError(f"Config file is empty or not found at: {config_path}")

        except Exception as e:
            # Catch any Spark-related file reading errors or initial empty content check
            logger.error(f"Failed to read config from {config_path} using Spark. Attempting dbutils fallback (for smaller files). Error: {e}")
            try:
                # Fallback to dbutils.fs.head for smaller files, if Spark read fails
                # Note: dbutils.fs.head has a default limit (e.g., 1MB)
                from dbutils import DBUtils
                dbutils_instance = DBUtils(spark.sparkContext)
                config_content = dbutils_instance.fs.head(config_path)
                if not config_content.strip():
                    raise FileNotFoundError(f"Config file is empty or not found at: {config_path} via dbutils.")
            except Exception as db_e:
                logger.error(f"Failed to read config from {config_path} using dbutils. Error: {db_e}")
                raise FileNotFoundError(f"Could not read config file from {config_path}. Check path and permissions.") from db_e

        try:
            config_dict = yaml.safe_load(config_content)
            print(config_dict)
            logger.info(f"Successfully loaded validation config from: {config_path}")
            print("Type of config_dict:", type(config_dict))
            runner = ValidationRunner(spark)
            validated_df = runner.run_validations(dataframe=dataframe, parsed_config=config_dict['validation_config'])
            return validated_df
            
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML config from {config_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during YAML parsing: {e}")
            raise

    
