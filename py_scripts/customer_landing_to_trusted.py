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

# Script generated for node Customer Landing
CustomerLanding_node1760391256332 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1760391256332")

# Script generated for node Manual Privacy Filter
SqlQuery811 = '''
select * from myDataSource
WHERE shareWithResearchAsOfDate IS NOT NULL
  AND shareWithResearchAsOfDate != 0;
'''
ManualPrivacyFilter_node1760391466452 = sparkSqlQuery(glueContext, query = SqlQuery811, mapping = {"myDataSource":CustomerLanding_node1760391256332}, transformation_ctx = "ManualPrivacyFilter_node1760391466452")

# Script generated for node Customer Trusted Zone
EvaluateDataQuality().process_rows(frame=ManualPrivacyFilter_node1760391466452, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760388624485", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrustedZone_node1760392797692 = glueContext.getSink(path="s3://bayou-bandit/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrustedZone_node1760392797692")
CustomerTrustedZone_node1760392797692.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrustedZone_node1760392797692.setFormat("glueparquet", compression="gzip")
CustomerTrustedZone_node1760392797692.writeFrame(ManualPrivacyFilter_node1760391466452)
job.commit()