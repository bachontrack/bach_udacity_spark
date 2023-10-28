import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1698430912558 = glueContext.create_dynamic_frame.from_catalog(
    database="bachhoang",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1698430912558",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1698432105879 = glueContext.create_dynamic_frame.from_catalog(
    database="bachhoang",
    table_name="step_trainer_trusted",
    transformation_ctx="steptrainertrusted_node1698432105879",
)

# Script generated for node SQL Query
SqlQuery486 = """
select distinct acc.user, acc.timestamp, acc.x, acc.y, acc.z
, sensorreadingtime, serialnumber, distancefromobject 
from acc
inner join step
on step.sensorreadingtime = acc.timestamp
"""
SQLQuery_node1698432118544 = sparkSqlQuery(
    glueContext,
    query=SqlQuery486,
    mapping={
        "acc": accelerometertrusted_node1698430912558,
        "step": steptrainertrusted_node1698432105879,
    },
    transformation_ctx="SQLQuery_node1698432118544",
)

# Script generated for node Amazon S3
AmazonS3_node1698432217404 = glueContext.getSink(
    path="s3://practicebucket193prac/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698432217404",
)
AmazonS3_node1698432217404.setCatalogInfo(
    catalogDatabase="bachhoang", catalogTableName="machine_learning_curated"
)
AmazonS3_node1698432217404.setFormat("json")
AmazonS3_node1698432217404.writeFrame(SQLQuery_node1698432118544)
job.commit()
