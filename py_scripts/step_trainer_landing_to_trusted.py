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

# Script generated for node Customer Curated
CustomerCurated_node1760560574165 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1760560574165")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1760560570970 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1760560570970")

# Script generated for node Manual Join
SqlQuery6141 = '''
SELECT s.*
FROM step_trainer_landing s
JOIN customer_curated c
  ON s.serialNumber = c.serialnumber
WHERE c.shareWithResearchAsOfDate IS NOT NULL

'''
ManualJoin_node1760560580156 = sparkSqlQuery(glueContext, query = SqlQuery6141, mapping = {"step_trainer_landing":StepTrainerLanding_node1760560570970, "customer_curated":CustomerCurated_node1760560574165}, transformation_ctx = "ManualJoin_node1760560580156")

# Script generated for node Drop Duplicates
SqlQuery6140 = '''
select distinct * from myDataSource

'''
DropDuplicates_node1760561722426 = sparkSqlQuery(glueContext, query = SqlQuery6140, mapping = {"myDataSource":ManualJoin_node1760560580156}, transformation_ctx = "DropDuplicates_node1760561722426")

# Script generated for node Step Trainer Trusted Zone
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1760561722426, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760560211452", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrustedZone_node1760561785153 = glueContext.getSink(path="s3://bayou-bandit/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrustedZone_node1760561785153")
StepTrainerTrustedZone_node1760561785153.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrustedZone_node1760561785153.setFormat("glueparquet", compression="gzip")
StepTrainerTrustedZone_node1760561785153.writeFrame(DropDuplicates_node1760561722426)
job.commit()