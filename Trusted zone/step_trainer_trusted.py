import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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
AmazonS3_node1760653959810 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_curated", transformation_ctx="AmazonS3_node1760653959810")

# Script generated for node Amazon S3
AmazonS3_node1760653960498 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="AmazonS3_node1760653960498")

# Script generated for node Join
AmazonS3_node1760653959810DF = AmazonS3_node1760653959810.toDF()
AmazonS3_node1760653960498DF = AmazonS3_node1760653960498.toDF()
Join_node1760654401969 = DynamicFrame.fromDF(AmazonS3_node1760653959810DF.join(AmazonS3_node1760653960498DF, (AmazonS3_node1760653959810DF['serialnumber'] == AmazonS3_node1760653960498DF['serialnumber']), "left"), glueContext, "Join_node1760654401969")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1760654401969, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760653944157", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760653964135 = glueContext.getSink(path="s3://stedi-project-dgs/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1760653964135")
AmazonS3_node1760653964135.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
AmazonS3_node1760653964135.setFormat("json")
AmazonS3_node1760653964135.writeFrame(Join_node1760654401969)
job.commit()