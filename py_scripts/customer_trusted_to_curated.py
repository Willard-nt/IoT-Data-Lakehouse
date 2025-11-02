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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1760556586740 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1760556586740")

# Script generated for node Customer Trusted
CustomerTrusted_node1760556548594 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1760556548594")

# Script generated for node Manual Join
SqlQuery5796 = '''
SELECT c.*
FROM customer_trusted c
JOIN accelerometer_trusted a
  ON c.email = a.user
WHERE c.shareWithResearchAsOfDate IS NOT NULL

'''
ManualJoin_node1760556677142 = sparkSqlQuery(glueContext, query = SqlQuery5796, mapping = {"customer_trusted":CustomerTrusted_node1760556548594, "accelerometer_trusted":AccelerometerTrusted_node1760556586740}, transformation_ctx = "ManualJoin_node1760556677142")

# Script generated for node Drop Duplicates
SqlQuery5795 = '''
select distinct * 
FROM myDataSource
'''
DropDuplicates_node1760558302155 = sparkSqlQuery(glueContext, query = SqlQuery5795, mapping = {"myDataSource":ManualJoin_node1760556677142}, transformation_ctx = "DropDuplicates_node1760558302155")

# Script generated for node Customer Curated Zone
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1760558302155, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760556518172", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCuratedZone_node1760559046788 = glueContext.getSink(path="s3://bayou-bandit/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCuratedZone_node1760559046788")
CustomerCuratedZone_node1760559046788.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCuratedZone_node1760559046788.setFormat("glueparquet", compression="gzip")
CustomerCuratedZone_node1760559046788.writeFrame(DropDuplicates_node1760558302155)
job.commit()