from sqlalchemy import create_engine
import psycopg2

class pgDB:

    def __init__(
            self,
            user,
            password,
            host,
            port,
            database
        ):
        engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )
        self.engine = engine
    
    def SparkDataFrameToDB(
            self, 
            TableNameDB,
            SparkDataFrame,
            if_exists,
            index
        ):
        SparkDataFrame.toPandas().to_sql(
            name=TableNameDB,
            con=self.engine,
            if_exists=if_exists,
            index=index
        )
        return self.engine.dispose()