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
accelerometertrusted_node1698429975373 = glueContext.create_dynamic_frame.from_catalog(
    database="bachhoang",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1698429975373",
)

# Script generated for node customer curated
customercurated_node1698429089286 = glueContext.create_dynamic_frame.from_catalog(
    database="bachhoang",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1698429089286",
)

# Script generated for node step trainer
steptrainer_node1698429065209 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://practicebucket193prac/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainer_node1698429065209",
)

# Script generated for node SQL Query
SqlQuery489 = """
select * from step
inner join cus
on cus.serialnumber = step.serialNumber
inner join acc
on acc.timestamp = step.sensorReadingTime
"""
SQLQuery_node1698429117523 = sparkSqlQuery(
    glueContext,
    query=SqlQuery489,
    mapping={
        "step": steptrainer_node1698429065209,
        "cus": customercurated_node1698429089286,
        "acc": accelerometertrusted_node1698429975373,
    },
    transformation_ctx="SQLQuery_node1698429117523",
)

# Script generated for node Amazon S3
AmazonS3_node1698429231662 = glueContext.getSink(
    path="s3://practicebucket193prac/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698429231662",
)
AmazonS3_node1698429231662.setCatalogInfo(
    catalogDatabase="bachhoang", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1698429231662.setFormat("json")
AmazonS3_node1698429231662.writeFrame(SQLQuery_node1698429117523)
job.commit()
