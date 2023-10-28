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

# Script generated for node customer trusted
customertrusted_node1698427814771 = glueContext.create_dynamic_frame.from_catalog(
    database="bachhoang",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1698427814771",
)

# Script generated for node accelerometer landing
accelerometerlanding_node1698428090101 = glueContext.create_dynamic_frame.from_catalog(
    database="bachhoang",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1698428090101",
)

# Script generated for node SQL Query
SqlQuery450 = """
select DISTINCT 
cus.birthDay, cus.email, cus.phone, cus.customerName, cus.lastUpdateDate, cus.registrationDate, cus.serialNumber, cus.shareWithFriendsAsOfDate, cus.shareWithPublicAsOfDate, cus.shareWithResearchAsOfDate
from cus
inner join acc 
on cus.email = acc.user

"""
SQLQuery_node1698428627866 = sparkSqlQuery(
    glueContext,
    query=SqlQuery450,
    mapping={
        "cus": customertrusted_node1698427814771,
        "acc": accelerometerlanding_node1698428090101,
    },
    transformation_ctx="SQLQuery_node1698428627866",
)

# Script generated for node Amazon S3
AmazonS3_node1698428179442 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1698428627866,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://practicebucket193prac/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1698428179442",
)

job.commit()
