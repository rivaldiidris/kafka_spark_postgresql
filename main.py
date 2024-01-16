from utils.spark_utils import spark
import pyspark.sql.functions as F
from utils.db_utils import pgDB
from utils.config import read_config
from pyspark.sql.functions import sum

SheetLoanLevel = spark(
    config_key="spark.jars.packages",
    config_value="com.crealytics:spark-excel_2.12:0.13.5"
).SparkReadExcels(
    fileFormat="com.crealytics.spark.excel",
    header="true",
    inferSchema="true",
    dataAddress="'Loan Level'!A1",
    locationFile="/home/jovyan/work/DE_Rand_test.xlsx"
)

SheetPartnerLevel = spark(
    config_key="spark.jars.packages",
    config_value="com.crealytics:spark-excel_2.12:0.13.5"
).SparkReadExcels(
    fileFormat="com.crealytics.spark.excel",
    header="true",
    inferSchema="true",
    dataAddress="'Partner Level'!A1",
    locationFile="/home/jovyan/work/DE_Rand_test.xlsx"
).na.drop(
    subset=[
        "partner",
        "name"
    ]
)

print(SheetLoanLevel.count())
print(SheetPartnerLevel.count())

# 1.A change schema
SheetLoanLevel = SheetLoanLevel \
    .withColumn("loan_amount", SheetLoanLevel.loan_amount.cast('int')) \
    .withColumn("current_dpd", SheetLoanLevel.current_dpd.cast('int')) \
    .withColumn("max_dpd", SheetLoanLevel.max_dpd.cast('int')) \
    .withColumn("loan_term", SheetLoanLevel.loan_term.cast('int'))

# 1.B The variable ‘status’ in ‘loan’ has a ‘Pending’ value. 
# with Python Code, change it into ‘SUBMITTED’.
SheetLoanLevel.select('status').distinct().collect()

SheetLoanLevel = SheetLoanLevel \
    .withColumn('status', F.when(SheetLoanLevel.status=='PENDING', 'SUBMITTED') \
    .otherwise(SheetLoanLevel.status))

SheetLoanLevel.select('status').distinct().collect()
# 1.C Ingest the data into the Database.

# Insert Loan Level Table
pgDB(
    user=read_config()['postgresql']['user'],
    password=read_config()['postgresql']['password'],
    host=read_config()['postgresql']['host'],
    port=int(read_config()['postgresql']['port']),
    database=read_config()['postgresql']['database'],
).SparkDataFrameToDB(
    TableNameDB='loan',
    SparkDataFrame=SheetLoanLevel,
    if_exists='replace',
    index=False
)

# Insert Partner Level Table
pgDB(
    user=read_config()['postgresql']['user'],
    password=read_config()['postgresql']['password'],
    host=read_config()['postgresql']['host'],
    port=int(read_config()['postgresql']['port']),
    database=read_config()['postgresql']['database'],
).SparkDataFrameToDB(
    TableNameDB='partner',
    SparkDataFrame=SheetPartnerLevel,
    if_exists='replace',
    index=False
)

# Using tables from task 1 (those 2 raw tables) write a python code to create a new summarize tables
# 2.A DPD Bucket (Current (0), DPD 1 – 30, DPD 31 – 60, DPD 61++).
SheetLoanLevel = SheetLoanLevel.withColumn(
    'dpd_category',
    F.when((F.col("current_dpd") == 0), "dpd_category0")\
    .when((F.col("current_dpd") >= 1) & (F.col('current_dpd') <= 30), "dpd_category130")\
    .when((F.col("current_dpd") >= 31) & (F.col('current_dpd') <= 60), "dpd_category3160")\
    .otherwise("dpd_category61more")
)

dpd_bucket_cat0 = SheetLoanLevel.select("loan_id", "borrower_id", "loan_amount", "dpd_category").filter(SheetLoanLevel.dpd_category == "dpd_category0")
dpd_bucket_cat1 = SheetLoanLevel.select("loan_id", "borrower_id", "loan_amount", "dpd_category").filter(SheetLoanLevel.dpd_category == "dpd_category130")
dpd_bucket_cat2 = SheetLoanLevel.select("loan_id", "borrower_id", "loan_amount", "dpd_category").filter(SheetLoanLevel.dpd_category == "dpd_category3160")
dpd_bucket_cat3 = SheetLoanLevel.select("loan_id", "borrower_id", "loan_amount", "dpd_category").filter(SheetLoanLevel.dpd_category == "dpd_category61more")

# kolektibilitas ojk 
SheetLoanLevel = SheetLoanLevel.withColumn(
    'kolektibilitas',
    F.when((F.col("current_dpd") == 0), "lancar")\
    .when((F.col("current_dpd") >= 1) & (F.col('current_dpd') <= 90), "dalam_perhatian_khusus")\
    .when((F.col("current_dpd") >= 91) & (F.col('current_dpd') <= 120), "kurang_lancar")\
    .when((F.col("current_dpd") >= 121) & (F.col('current_dpd') <= 180), "diragukan")\
    .otherwise("macet")
)
# 2.B Loan term per DPD bucket.
loan_per_dpd = SheetLoanLevel.select("loan_term", "dpd_category")

# 2.C Calculate the number of NPL loans.

# membagi jumlah kredit kurang lancar, diragukan, dan macet dengan total kredit yang disalurkan, 
# kemudian dikali 100%

# Rasio NPL= (Total NPL (Kurang Lancar + Diragukan + Macet)/Total Kredit) X 100%
PL = SheetLoanLevel.filter(SheetLoanLevel.kolektibilitas == "lancar").select(sum(dpd_bucket_cat0.loan_amount)).first()[0]
NPL1 = SheetLoanLevel.filter(SheetLoanLevel.kolektibilitas == "dalam_perhatian_khusus").select(sum(dpd_bucket_cat0.loan_amount)).first()[0]
NPL2 = SheetLoanLevel.filter(SheetLoanLevel.kolektibilitas == "kurang_lancar").select(sum(dpd_bucket_cat0.loan_amount)).first()[0]
NPL3 = SheetLoanLevel.filter(SheetLoanLevel.kolektibilitas == "diragukan").select(sum(dpd_bucket_cat0.loan_amount)).first()[0]
NPL4 = 0 if (SheetLoanLevel.filter(SheetLoanLevel.kolektibilitas == "macet").select(sum(dpd_bucket_cat0.loan_amount)).first()[0]) == None else (SheetLoanLevel.filter(SheetLoanLevel.kolektibilitas == "macet").select(sum(dpd_bucket_cat0.loan_amount)).first()[0])

Total_NPL = NPL2 + NPL3 + NPL4
Credit_Total = PL + NPL1 + NPL2 + NPL3 + NPL4
NPL = round((Total_NPL / Credit_Total) * 100, 2)
print(f"NPL NUMBER : {NPL}%")