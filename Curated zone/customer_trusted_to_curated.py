import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1760651450603 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="AmazonS3_node1760651450603")

# Script generated for node Amazon S3
AmazonS3_node1760651451713 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="AmazonS3_node1760651451713")

# Script generated for node Join
Join_node1760651454736 = Join.apply(frame1=AmazonS3_node1760651451713, frame2=AmazonS3_node1760651450603, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1760651454736")

# Script generated for node Drop Fields and Duplicates
SqlQuery5956 = '''
select distinct customerName, email, phone, birthDay, 
serialNumber, registrationDate, lastUpdateDate, shareWithResearchAsOfDate,
shareWithPublicAsOfDate, shareWithFriendsAsOfDate from myDataSource

'''
DropFieldsandDuplicates_node1760652978966 = sparkSqlQuery(glueContext, query = SqlQuery5956, mapping = {"myDataSource":Join_node1760651454736}, transformation_ctx = "DropFieldsandDuplicates_node1760652978966")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1760652978966, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760651443381", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760651464733 = glueContext.getSink(path="s3://stedi-project-dgs/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1760651464733")
AmazonS3_node1760651464733.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
AmazonS3_node1760651464733.setFormat("json")
AmazonS3_node1760651464733.writeFrame(DropFieldsandDuplicates_node1760652978966)
job.commit()