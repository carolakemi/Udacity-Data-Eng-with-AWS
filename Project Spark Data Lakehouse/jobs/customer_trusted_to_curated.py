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

# Script generated for node customer trusted
customertrusted_node1747157198965 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-data-lakehouse/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1747157198965")

# Script generated for node accelerometer trusted
accelerometertrusted_node1747157257622 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-data-lakehouse/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1747157257622")

# Script generated for node Join
Join_node1747157596078 = Join.apply(frame1=accelerometertrusted_node1747157257622, frame2=customertrusted_node1747157198965, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1747157596078")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct customerName, email, phone, birthday, serialNumber, registrationDate, lastUpdateDate, shareWithResearchAsOfDate, shareWithPublicAsOfDate, shareWithFriendsAsOfDate
from myDataSource
'''
SQLQuery_node1747157669051 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1747157596078}, transformation_ctx = "SQLQuery_node1747157669051")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1747157669051, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747153057336", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747157878162 = glueContext.getSink(path="s3://project-data-lakehouse/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1747157878162")
AmazonS3_node1747157878162.setCatalogInfo(catalogDatabase="project-data-lakehouse",catalogTableName="customer_curated")
AmazonS3_node1747157878162.setFormat("json")
AmazonS3_node1747157878162.writeFrame(SQLQuery_node1747157669051)
job.commit()