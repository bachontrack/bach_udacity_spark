import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1698415826948 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://practicebucket193prac/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1698415826948",
)

# Script generated for node Filter
Filter_node1698416049267 = Filter.apply(
    frame=CustomerLanding_node1698415826948,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1698416049267",
)

# Script generated for node Amazon S3
AmazonS3_node1698416286996 = glueContext.getSink(
    path="s3://practicebucket193prac/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698416286996",
)
AmazonS3_node1698416286996.setCatalogInfo(
    catalogDatabase="bachhoang", catalogTableName="customer_trusted"
)
AmazonS3_node1698416286996.setFormat("json")
AmazonS3_node1698416286996.writeFrame(Filter_node1698416049267)
job.commit()
