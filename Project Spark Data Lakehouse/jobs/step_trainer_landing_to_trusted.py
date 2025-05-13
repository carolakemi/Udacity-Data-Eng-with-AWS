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

# Script generated for node customer curated
customercurated_node1747158247778 = glueContext.create_dynamic_frame.from_catalog(database="project-data-lakehouse", table_name="customer_curated", transformation_ctx="customercurated_node1747158247778")

# Script generated for node step trainer landing
steptrainerlanding_node1747158484958 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-data-lakehouse/step_trainer/landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1747158484958")

# Script generated for node SQL Query
SqlQuery1 = '''
select * from stl
join cc on stl.serialNumber = cc.serialNumber
'''
SQLQuery_node1747160590598 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"cc":customercurated_node1747158247778, "stl":steptrainerlanding_node1747158484958}, transformation_ctx = "SQLQuery_node1747160590598")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct sensorReadingTime, serialNumber, distanceFromObject from myDataSource
'''
SQLQuery_node1747158994294 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":SQLQuery_node1747160590598}, transformation_ctx = "SQLQuery_node1747158994294")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1747158994294, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747158154817", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747159125902 = glueContext.getSink(path="s3://project-data-lakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1747159125902")
AmazonS3_node1747159125902.setCatalogInfo(catalogDatabase="project-data-lakehouse",catalogTableName="step_trainer_trusted")
AmazonS3_node1747159125902.setFormat("json")
AmazonS3_node1747159125902.writeFrame(SQLQuery_node1747158994294)
job.commit()