from pyspark.sql import SparkSession

class spark:

    def __init__(
            self, 
            config_key, 
            config_value
        ):
        sparkClient = SparkSession.builder \
            .config(f"{config_key}", f"{config_value}") \
            .master("local").appName("SparkApps").getOrCreate()
        self.sparkClient = sparkClient
    
    def SparkReadExcels(
            self, 
            fileFormat, 
            header, 
            inferSchema, 
            dataAddress, 
            locationFile
        ):
        sparkDF = self.sparkClient.read.format(f"{fileFormat}") \
        .option("header", f"{header}") \
        .option("inferSchema", f"{inferSchema}") \
        .option("dataAddress", f"{dataAddress}") \
        .load(f"{locationFile}")
        return sparkDF