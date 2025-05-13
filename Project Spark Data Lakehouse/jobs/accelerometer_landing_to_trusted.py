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

# Script generated for node accelerometer landing
accelerometerlanding_node1747155987286 = glueContext.create_dynamic_frame.from_catalog(database="project-data-lakehouse", table_name="accelerometer_landing", transformation_ctx="accelerometerlanding_node1747155987286")

# Script generated for node customer trusted
customertrusted_node1747156341975 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-data-lakehouse/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1747156341975")

# Script generated for node Join
Join_node1747156508661 = Join.apply(frame1=customertrusted_node1747156341975, frame2=accelerometerlanding_node1747155987286, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1747156508661")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct user, timestamp, x, y, z
from myDataSource
'''
SQLQuery_node1747156561777 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1747156508661}, transformation_ctx = "SQLQuery_node1747156561777")

# Script generated for node accelerometer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1747156561777, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747154428383", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1747156644170 = glueContext.getSink(path="s3://project-data-lakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1747156644170")
accelerometertrusted_node1747156644170.setCatalogInfo(catalogDatabase="project-data-lakehouse",catalogTableName="accelerometer_trusted")
accelerometertrusted_node1747156644170.setFormat("json")
accelerometertrusted_node1747156644170.writeFrame(SQLQuery_node1747156561777)
job.commit()