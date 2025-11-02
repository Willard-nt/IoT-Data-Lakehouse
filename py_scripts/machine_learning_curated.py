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
AccelerometerTrusted_node1760570910600 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1760570910600")

# Script generated for node Customer Curated
CustomerCurated_node1760570911348 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1760570911348")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1760570908492 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1760570908492")

# Script generated for node Manual Join
SqlQuery5790 = '''
SELECT
s.sensorReadingTime,
s.serialNumber,
s.distanceFromObject,
a.x,
a.y,
a.z
FROM step_trainer_trusted s
JOIN accelerometer_trusted a
ON a.`timeStamp` = s.sensorReadingTime
JOIN customer_curated c
ON s.serialNumber = c.serialnumber
WHERE shareWithResearchAsOfDate IS NOT NULL;
'''
ManualJoin_node1760570917490 = sparkSqlQuery(glueContext, query = SqlQuery5790, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1760570908492, "accelerometer_trusted":AccelerometerTrusted_node1760570910600, "customer_curated":CustomerCurated_node1760570911348}, transformation_ctx = "ManualJoin_node1760570917490")

# Script generated for node Machine Learning Curated Zone
EvaluateDataQuality().process_rows(frame=ManualJoin_node1760570917490, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760570783119", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCuratedZone_node1760570926769 = glueContext.getSink(path="s3://bayou-bandit/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCuratedZone_node1760570926769")
MachineLearningCuratedZone_node1760570926769.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCuratedZone_node1760570926769.setFormat("glueparquet", compression="gzip")
MachineLearningCuratedZone_node1760570926769.writeFrame(ManualJoin_node1760570917490)
job.commit()