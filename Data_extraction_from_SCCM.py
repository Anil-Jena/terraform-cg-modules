###Prerequisites####
# STEP-1
# 1. Need the SQL Server hostname, database name, username, and password for your SCCM database.
# 2. AWS Glue needs to be able to connect to your SCCM SQL Server. (Tufin request)
# 3. Ensure that the IAM role assigned to your Glue job has the necessary permissions, such as access to Amazon S3 (for storing results) and other Glue resources.

#Step 2: Create a JDBC Connection in AWS Glue
#Before you create the Glue job, set up a JDBC connection for the SCCM SQL Server.
# 1. Navigate to AWS Glue in the AWS Console.
# 2. Go to Connections under the Data Catalog section and click on Add connection.
# 3. Choose JDBC as the connection type.
# 4. Fill in the required details:
    # Name: Name your connection (e.g., sccm-sql-connection).
    # JDBC URL: Provide the connection string for the SQL Server. It will look something like:
    # jdbc:sqlserver://your-sccm-server.database.windows.net:1433;databaseName=SCCMDatabase
    # Username and Password: Provide the credentials to access the SQL Server database.



#===Option-1==============================================================================
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext

# Initialize the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
sqlContext = SQLContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Connect to the SCCM database using the connection created in Glue
jdbc_url = "jdbc:sqlserver://your-sccm-server.database.windows.net:1433;databaseName=SCCMDatabase"
dbtable = "dbo.v_R_System"  # Table from SCCM
user = "your_db_username"
password = "your_db_password"

# Read data from SCCM SQL Server
df = sqlContext.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", dbtable) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

# Show the schema and data
df.printSchema()
df.show()

# Write the data to an S3 bucket
s3_output_path = "s3://your-s3-bucket/sccm-data/"
df.write.mode("overwrite").parquet(s3_output_path)

# Commit the Glue job
job.commit()




#===Option-2==============================================================================
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Glue boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# SCCM SQL Server connection parameters
jdbc_url = "jdbc:sqlserver://<SQL_SERVER_HOST>:<PORT>;databaseName=<DATABASE_NAME>"
db_table = "dbo.v_R_System"  # Example SCCM table to query
db_properties = {
    "user": "your_db_username",
    "password": "your_db_password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Load data from SCCM SQL Server
sccm_df = spark.read.jdbc(url=jdbc_url, table=db_table, properties=db_properties)

# Show the retrieved data (optional, for debugging)
sccm_df.show()

# Example: Write the data to an S3 bucket in CSV format
sccm_df.write.format("csv").option("header", "true").save("s3://your-s3-bucket/sccm-data/")

# Commit the Glue job
job.commit()
