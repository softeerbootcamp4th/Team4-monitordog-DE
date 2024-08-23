from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, count, lag
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
import boto3

# S3 버킷 및 디렉토리 정보
bucket_name = {bucket_name}
directory_path = {directory_path}
# load
s3_temp_dir = {S3_PATH}
username = {username}
password = {password}
redshift_jdbc_url = f"{REDSHIFT_JDBC_URL}?user={username}&password={password}"
redshift_iam_role = {IAM_ROLE_ARN}
raw_table_name = {RAW_TABLE_NAME}
history_table_name = {HISTORY_TABLE_NAME}

# S3 파일 목록 가져오기
s3 = boto3.client('s3')
file_list = s3.list_objects(Bucket=bucket_name, Prefix=directory_path)['Contents']

# SparkSession 생성
spark = SparkSession.builder \
    .appName("comments schema handling") \
    .getOrCreate()

# comments 필드 스키마 정의
schema_with_string_children = ArrayType(
    StructType([
        StructField("author", StringType(), True),
        StructField("children", ArrayType(StringType()), True),
        StructField("content", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("num_of_comments", StringType(), True),
    ])
)

schema_with_struct_children = ArrayType(
    StructType([
        StructField("author", StringType(), True),
        StructField("children", ArrayType(
            StructType([
                StructField("author", StringType(), True),
                StructField("content", StringType(), True),
                StructField("created_at", StringType(), True),
            ])
        ), True),
        StructField("content", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("num_of_comments", StringType(), True),
    ])
)

# UDF 정의
def unify_comments_schema(comments):
    if comments is None:
        return []

    unified_comments = []
    for comment in comments:
        if isinstance(comment["children"], list):
            if len(comment["children"]) > 0 and isinstance(comment["children"][0], dict):
                # Struct 형태로 통일
                unified_comment = {
                    "author": comment["author"],
                    "children": comment["children"],
                    "content": comment["content"],
                    "created_at": comment["created_at"],
                    "num_of_comments": comment["num_of_comments"],
                }
            else:
                # String 형태의 children을 빈 Struct로 변환
                unified_comment = {
                    "author": comment["author"],
                    "children": [],
                    "content": comment["content"],
                    "created_at": comment["created_at"],
                    "num_of_comments": comment["num_of_comments"],
                }
            unified_comments.append(unified_comment)
    return unified_comments

# UDF 등록
unify_comments_schema_udf = spark.udf.register("unify_comments_schema", unify_comments_schema, schema_with_struct_children)

# 파일 목록을 시간순으로 정렬
file_list.sort(key=lambda x: x['LastModified'])

# 각 파일을 데이터프레임으로 읽어들이고 시간순으로 병합
dfs = []
for file_info in file_list:
    file_path = f's3://{bucket_name}/{file_info["Key"]}'
    df = spark.read.json(file_path)
    df = df.withColumn('file_created_time', F.lit(file_info['LastModified']))
    df = df.withColumn("comments", unify_comments_schema_udf(col("comments")))
    dfs.append(df)


merged_df = reduce(DataFrame.unionAll, dfs)
merged_df = merged_df.orderBy('file_created_time')


merged_df.dropna(how='any', subset=['title', 'content'])
merged_df = merged_df.filter(merged_df.title.isNotNull())

# 이전 데이터프레임의 게시글 수를 가져오기 위해 lag 함수 사용
post_hisotry = merged_df.groupBy('file_created_time').agg(count('title').alias('post_count'))

post_hisotry = post_hisotry.withColumn('prev_post_count', lag('post_count', 1).over(Window.orderBy('file_created_time')))
post_hisotry = post_hisotry.withColumn('post_count_diff', post_hisotry.post_count - post_hisotry.prev_post_count)

merged_df.write \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", redshift_jdbc_url) \
    .option("dbtable", raw_table_name) \
    .option("tempdir", s3_temp_dir) \
    .option("tempformat", "CSV GZIP") \
    .option("aws_iam_role", redshift_iam_role) \
    .mode("append") \
    .save()

post_hisotry.write \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", redshift_jdbc_url) \
    .option("dbtable", history_table_name) \
    .option("tempdir", s3_temp_dir) \
    .option("tempformat", "CSV GZIP") \
    .option("aws_iam_role", redshift_iam_role) \
    .mode("overwrite") \
    .save()
