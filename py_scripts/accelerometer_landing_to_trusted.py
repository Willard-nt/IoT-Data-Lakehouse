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

# Script generated for node Customer Trusted
CustomerTrusted_node1760463194247 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1760463194247")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1760397258868 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1760397258868")

# Script generated for node SQL Query
SqlQuery5015 = '''
SELECT a.*
FROM accelerometer_landing a
JOIN customer_trusted c
  ON a.user = c.email
WHERE c.shareWithResearchAsOfDate IS NOT NULL

'''
SQLQuery_node1760467701531 = sparkSqlQuery(glueContext, query = SqlQuery5015, mapping = {"accelerometer_landing":AccelerometerLanding_node1760397258868, "customer_trusted":CustomerTrusted_node1760463194247}, transformation_ctx = "SQLQuery_node1760467701531")

# Script generated for node Accelerometer Trusted Zone
EvaluateDataQuality().process_rows(frame=SQLQuery_node1760467701531, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760396731960", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrustedZone_node1760397281093 = glueContext.getSink(path="s3://bayou-bandit/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrustedZone_node1760397281093")
AccelerometerTrustedZone_node1760397281093.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrustedZone_node1760397281093.setFormat("glueparquet", compression="gzip")
AccelerometerTrustedZone_node1760397281093.writeFrame(SQLQuery_node1760467701531)
job.commit()