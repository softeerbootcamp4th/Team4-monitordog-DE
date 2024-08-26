# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import sys
import boto3
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col,
    explode,
    sum,
    udf,
    expr,
    broadcast,
    to_timestamp,
    collect_list,
    lit,
    from_json,
    date_sub,
    sequence,
    substring,
)
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    ArrayType,
    StringType,
)
import json
from math import log
from scipy.spatial.distance import euclidean
from scipy.stats import zscore
from fastdtw import fastdtw
import numpy as np

BUCKET_NAME = "monitordog-data"
DIRECTORY_PATH = "keywords"

USERNAME = "monitordog-awsuser"
PASSWORD = "Monitordog1234!!"
REDSHIFT_JDBC_URL = (
    f"jdbc:redshift://10.0.0.8:5439/monitordog-dev?user={USERNAME}&password={PASSWORD}"
)
REDSHIFT_IAM_ROLE = "arn:aws:iam::367354627828:role/service-role/AmazonRedshift-CommandsAccessRole-20240819T031044"

S3_TEMP_DIR = "s3://ex-emr/temp/"


"""
    -- udf로 등록해서 쓴 함수 --

    dtw_similarity_score(x, y, radius: int = 1) -> float:
    get_issue_score(viewed: float, liked: float, num_of_comments: float) -> float:
"""


# DTW 유사도 함수 정의
def dtw_similarity_score(x, y, radius: int = 1) -> float:
    """
    정규화된 DTW 유사도 계산
    :param x: 그래프를 나타내는 1차원 리스트
    :param y: 그래프를 나타내는 1차원 리스트, x와 길이가 동일해야 정확한 값이 나옴
    :param radius: Fast DTW 알고리즘에서 경로를 탐색할 범위
    :return: [0,1] 사이의 실수값, 1에 가까울수록 유사한 그래프
    """
    if not isinstance(x, list) or not isinstance(y, list):
        raise TypeError("입력은 list 형태여야 합니다.")

    x = list(x)[:1440]
    y = list(y)[:1440]

    raw_distance, _ = fastdtw(
        list(enumerate(x)), list(enumerate(y)), dist=euclidean, radius=radius
    )

    max_seq = max(max(x), max(y))
    min_seq = min(min(x), min(y))
    max_distance = abs(max_seq - min_seq) * max(len(x), len(y))

    normalized_distance = raw_distance / max_distance

    similarity_score = 1 - normalized_distance

    return similarity_score


def get_issue_score(viewed: float, liked: float, num_of_comments: float) -> float:
    """
    특정 시간대의 게시글들의 이슈화 정도를 점수화
    :param viewed: 특정 시간대 내 게시글들의 조회수 총합
    :param liked: 특정 시간대 내 추천 수 총합
    :param num_of_comments: 특정 시간대 내 댓글 수 총합
    :return: 특정 시간대에서의 이슈화 점수
    """
    return viewed + log(1 + liked) + log(1 + num_of_comments)


# UDF 등록
issueization_udf = udf(get_issue_score, DoubleType())
dtw_similarity_score_udf = udf(dtw_similarity_score, FloatType())


"""
    -- redshift에서 읽거나 쓰기는 함수 --

    read_from_redshift(spark, data)
        - 필요한 과거 이슈 데이터, 비교할 두달치 데이터 읽기
    load_to_redshift(df, data)
        - 그래프로 보여줄 데이터 쓰기
    delete_issue_graph_view_and_keyword_frequency_view()
        - 그래프로 나타낼 뷰 삭제
    create_issue_graph_view_and_keyword_frequency_view()
        - 그래프로 나타낼 뷰 생성
"""


def read_from_redshift(spark, data):
    if data == "raw_data_df":
        raw_data_df = (
            spark.read.format("io.github.spark_redshift_community.spark.redshift")
            .option("url", REDSHIFT_JDBC_URL)
            .option("dbtable", "raw_data")
            .option("tempdir", S3_TEMP_DIR)
            .option("aws_iam_role", REDSHIFT_IAM_ROLE)
            .load()
        )
        return raw_data_df
    elif data == "past_issue":
        past_issue_df = (
            spark.read.format("io.github.spark_redshift_community.spark.redshift")
            .option("url", REDSHIFT_JDBC_URL)
            .option("dbtable", "past_issue")
            .option("tempdir", S3_TEMP_DIR)
            .option("aws_iam_role", REDSHIFT_IAM_ROLE)
            .load()
        )
        return past_issue_df


def load_to_redshift(df, data):
    if data == "raw_data_df":
        print("save raw_data_df")
        df.write.format("io.github.spark_redshift_community.spark.redshift").option(
            "url", REDSHIFT_JDBC_URL
        ).option("dbtable", "raw_data").option("tempdir", S3_TEMP_DIR).option(
            "tempformat", "CSV GZIP"
        ).option(
            "aws_iam_role", REDSHIFT_IAM_ROLE
        ).mode(
            "append"
        ).save()
    elif data == "view_raw_data_df":
        print("save view_raw_data_df")
        df.write.format("io.github.spark_redshift_community.spark.redshift").option(
            "url", REDSHIFT_JDBC_URL
        ).option("dbtable", "raw_data_view").option("tempdir", S3_TEMP_DIR).option(
            "tempformat", "CSV GZIP"
        ).option(
            "aws_iam_role", REDSHIFT_IAM_ROLE
        ).mode(
            "overwrite"
        ).save()
    elif data == "current_issue_df":
        print("save current_issue_df")
        df.write.format("io.github.spark_redshift_community.spark.redshift").option(
            "url", REDSHIFT_JDBC_URL
        ).option("dbtable", "current_issue").option("tempdir", S3_TEMP_DIR).option(
            "tempformat", "CSV GZIP"
        ).option(
            "aws_iam_role", REDSHIFT_IAM_ROLE
        ).mode(
            "overwrite"
        ).save()
    elif data == "frequency_df":
        print("save frequency_df")
        df.write.format("io.github.spark_redshift_community.spark.redshift").option(
            "url", REDSHIFT_JDBC_URL
        ).option("dbtable", "current_issue_frequency").option(
            "tempdir", S3_TEMP_DIR
        ).option(
            "tempformat", "CSV GZIP"
        ).option(
            "aws_iam_role", REDSHIFT_IAM_ROLE
        ).mode(
            "overwrite"
        ).save()
    if data == "similar_df":
        print("save similar_df")
        df.write.format("io.github.spark_redshift_community.spark.redshift").option(
            "url", REDSHIFT_JDBC_URL
        ).option("dbtable", "similarity").option("tempdir", S3_TEMP_DIR).option(
            "tempformat", "CSV GZIP"
        ).option(
            "aws_iam_role", REDSHIFT_IAM_ROLE
        ).mode(
            "overwrite"
        ).save()


def delete_issue_graph_view_and_keyword_frequency_view():

    client = boto3.client("redshift-data", region_name="ap-northeast-2")

    cluster_id = "monitordog-redshift"
    database = "monitordog-dev"
    db_user = "monitordog-awsuser"

    issue_graph_view_sql_query = f"DROP VIEW IF EXISTS issue_graph_view;"

    response = client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=issue_graph_view_sql_query,
    )

    keyword_frequency_view_sql_query = f"DROP VIEW IF EXISTS keyword_frequency_view;"

    response = client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=keyword_frequency_view_sql_query,
    )


def create_issue_graph_view_and_keyword_frequency_view():

    client = boto3.client("redshift-data", region_name="ap-northeast-2")

    cluster_id = "monitordog-redshift"
    database = "monitordog-dev"
    db_user = "monitordog-awsuser"

    issue_graph_view_sql_query = f"""
        create view issue_graph_view as
        select *
        from (
            SELECT 
                DATEADD(SECOND,
                    (SELECT 
                        DATEDIFF(SECOND, created_at, DATE_TRUNC('hour', DATEADD(month, -2, GETDATE()))) AS diff
                    FROM past_issue
                    WHERE past_issue_name = 'g80 급발진'
                    ORDER BY created_at
                    LIMIT 1),
                created_at) AS created_at, 
                past_issue_name,
                past_issueization,
                past_sentiment
            FROM past_issue
            ORDER BY created_at
        ) as t1
        join (
            SELECT 
                DATEADD(SECOND,
                    (SELECT 
                        DATEDIFF(SECOND, created_at, DATE_TRUNC('hour', DATEADD(month, -2, GETDATE()))) AS diff
                    FROM current_issue
                    ORDER BY created_at
                    LIMIT 1),
                created_at) AS created_at, 
                current_keyword,
                current_issueization,
                num_of_comments,
                viewed,
                liked,
                sentiment
            FROM current_issue
            ORDER BY created_at
        ) as t2
        using (created_at)
    """

    response = client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=issue_graph_view_sql_query,
    )

    keyword_frequency_view_sql_query = f"""
        create view keyword_frequency_view as
        select current_keyword, sum(current_issueization) as frequency
        from current_issue
        group by current_keyword;
        """

    response = client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=keyword_frequency_view_sql_query,
    )


"""
    -- transform과 관련된 함수 --
    
    make_raw_df(df, timestamp)
        - 2달치 가져온 데이터 타입 변환

    make_keyword_df(df)
        - 현재 2일치 키워드 추출

    make_raw_data_df(spark)
        - 현재 2일치 데이터 저장하도록 변환
     
    make_issue_df(keyword_df, raw_data_df, start_date, end_date)
        - 현재 이슈화를 계산할 df 생성

    make_time_keywords_df(spark, keyword_df, start_date, end_date)
        - 그래프 비교하기위해 시간 보정

    make_current_issue_df(time_keywords_df, issue_df) 
        - 한시간 단위로 키워드 이슈화 계산
    
    make_frequency_df(current_issue_df)
        - 두달동안의 이슈화 정도 계산
    
    get_similarity(spark, current_issue_df)
        - 과거 이슈와 현재 키워드 유사도 비교
"""


def make_raw_df(df, timestamp):

    df = df.withColumn("file_create_time", lit(timestamp))

    df = df.withColumn("comments", col("comments").cast("string"))
    df = df.withColumn("keywords", col("keywords").cast("string"))
    df = df.withColumn("created_at", col("created_at").cast("timestamp"))
    df = df.withColumn("file_create_time", col("file_create_time").cast("timestamp"))
    df = df.withColumn("viewed", col("viewed").cast("integer"))
    df = df.withColumn("liked", col("liked").cast("integer"))
    df = df.withColumn("num_of_comments", col("num_of_comments").cast("integer"))
    df = df.withColumn("sentiment", col("sentiment").cast("float"))
    raw_df = df.select(
        "title",
        "content",
        "author",
        "created_at",
        "viewed",
        "liked",
        "num_of_comments",
        "comments",
        "model",
        "data_source",
        "keywords",
        "file_create_time",
        "sentiment",
        "url",
    )
    return raw_df


def make_keyword_df(df):

    # `keyword` 컬럼을 explode하여 모든 키워드를 행으로 펼치기
    exploded_df = df.select(explode(col("keywords")).alias("current_keyword"))

    # 유니크한 키워드만 포함된 DataFrame을 생성
    keyword_df = exploded_df.select("current_keyword").distinct()

    return keyword_df


def make_raw_data_df(spark):
    raw_data_df = read_from_redshift(spark, "raw_data_df")

    all_columns = raw_data_df.columns
    raw_data_df = raw_data_df.withColumn(
        "keywords", from_json(col("keywords"), ArrayType(StringType()))
    )
    # `keyword` 컬럼을 explode하여 모든 키워드를 행으로 펼치기
    raw_data_df = raw_data_df.select(
        explode(col("keywords")).alias("keywords"),
        *[
            col(c) for c in all_columns if c != "keywords"
        ],  # 'keywords' 컬럼을 제외한 나머지 모든 컬럼
    )

    return raw_data_df


def make_issue_df(keyword_df, raw_data_df, start_date, end_date):

    joined_df = raw_data_df.join(
        broadcast(keyword_df),
        raw_data_df["keywords"] == keyword_df["current_keyword"],
    )

    filtered_df = joined_df.filter(
        (col("file_create_time") >= start_date) & (col("file_create_time") <= end_date)
    )

    # 여기서 감정 그래프 추가
    issue_df = (
        filtered_df.groupBy("file_create_time", "current_keyword")
        .agg({"num_of_comments": "sum", "viewed": "sum", "liked": "sum", "sentiment": "sum"})
        .withColumnRenamed("sum(num_of_comments)", "num_of_comments")
        .withColumnRenamed("sum(viewed)", "viewed")
        .withColumnRenamed("sum(liked)", "liked")
        .withColumnRenamed("sum(sentiment)", "sentiment")
    )

    issue_df = issue_df.withColumn(
        "current_issueization",
        issueization_udf(col("num_of_comments"), col("viewed"), col("liked")),
    )

    issue_df = issue_df.orderBy("file_create_time")

    issue_df = issue_df.withColumn(
        "file_create_time", to_timestamp(col("file_create_time"), "yyyy-MM-dd HH:mm:ss")
    )

    return issue_df


def make_time_keywords_df(spark, keyword_df, start_date, end_date):

    # 전체 시간 범위를 생성 (1시간 단위)
    time_range_df = spark.range(0, 1).select(
        explode(sequence(start_date, end_date, expr("INTERVAL 1 HOUR"))).alias(
            "file_create_time"
        )
    )

    # 시간 범위와 키워드 조합
    time_keywords_df = time_range_df.crossJoin(keyword_df)
    time_keywords_df = time_keywords_df.withColumn("current_issueization", lit(0))

    return time_keywords_df


def make_current_issue_df(time_keywords_df, issue_df):

    joined_df = time_keywords_df.alias("tk").join(
        issue_df.alias("i"),
        (col("tk.file_create_time") == col("i.file_create_time"))
        & (col("tk.current_keyword") == col("i.current_keyword")),
        "left",
    )

    current_issue_df = joined_df.select(
        col("tk.file_create_time").alias("created_at"),
        col("tk.current_keyword"),
        col("i.current_issueization").alias("current_issueization"),
        col("i.num_of_comments"),
        col("i.viewed"),
        col("i.liked"),
        col("i.sentiment")
    )

    current_issue_df = current_issue_df.fillna(
        {"current_issueization": 0, "num_of_comments": 0, "viewed": 0, "liked": 0, "sentiment": 0}
    )

    current_issue_df = current_issue_df.withColumn(
        "current_issueization",
        issueization_udf(col("viewed"), col("liked"), col("num_of_comments")),
    )

    current_issue_df = current_issue_df.orderBy("created_at")

    return current_issue_df


def make_frequency_df(current_issue_df):
    # current_keyword로 그룹화하고 current_issueization의 합계에 로그 적용
    frequency_df = current_issue_df.groupBy("current_keyword").agg(
        F.log(sum("current_issueization") + 1).alias(
            "total_frequency"
        )  # +1을 하는 이유는 로그 계산에서 0을 방지하기 위해서
    )

    return frequency_df


def get_current_issue_and_raw_data(spark, df, timestamp):

    end_date = to_timestamp(lit(timestamp), "yyyy-MM-dd HH:mm:ss")
    start_date = date_sub(end_date, 62)

    raw_df = make_raw_df(df, timestamp)

    keyword_df = make_keyword_df(df)

    raw_data_df = make_raw_data_df(spark)

    view_raw_data_df = raw_data_df.withColumn(
        "content", substring(col("content"), 1, 255)
    ).withColumn("comments", substring(col("comments"), 1, 255))

    issue_df = make_issue_df(keyword_df, raw_data_df, start_date, end_date)

    time_keywords_df = make_time_keywords_df(spark, keyword_df, start_date, end_date)

    current_issue_df = make_current_issue_df(time_keywords_df, issue_df)

    frequency_df = make_frequency_df(current_issue_df)

    return raw_df, view_raw_data_df, current_issue_df, frequency_df


def get_similarity(spark, current_issue_df):

    keyword_issueization_df = current_issue_df.groupBy("current_keyword").agg(
        collect_list("current_issueization").alias("current_issueization")
    )

    past_issue_df = read_from_redshift(spark, "past_issue")

    keyword_issueization_df2 = past_issue_df.groupBy("past_issue_name").agg(
        collect_list("past_issueization").alias("past_issueization")
    )

    broadcast_df2 = broadcast(keyword_issueization_df2)

    similar_df = keyword_issueization_df.crossJoin(broadcast_df2).withColumn(
        "similarity_degree",
        dtw_similarity_score_udf(col("past_issueization"), col("current_issueization")),
    )

    similar_df = similar_df.drop("current_issueization", "past_issueization")
    # similar_df = similar_df.withColumn(
    #     "similarity_degree", col("similarity_degree").cast("float")
    # )

    return similar_df


def extract(spark):

    recent_time = None

    # 전달된 매개변수 파싱
    for i, arg in enumerate(sys.argv):
        if arg == "--recent_time":
            recent_time = sys.argv[i + 1]

    print(recent_time)

    prefix = f"{DIRECTORY_PATH}/{recent_time}"
    # prefix = f"{DIRECTORY_PATH}/2024-08-24T19:00:00"

    print(prefix)

    s3 = boto3.client("s3")
    file_list = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix)["Contents"]
    file_list.sort(key=lambda x: x["LastModified"], reverse=True)
    keys = [file["Key"] for file in file_list][:4]

    for key in keys:
        print(key)

    # 첫 번째 파일 읽기
    df = spark.read.json(f"s3a://{BUCKET_NAME}/{keys[0]}")
    df = df.dropDuplicates(["url"])
    df = df.withColumn("comments", col("comments").cast("string"))
    if "modified_at" in df.columns:
        df = df.drop("modified_at")

    # 나머지 파일들을 union
    for key in keys[1:]:
        df2 = spark.read.json(f"s3a://{BUCKET_NAME}/{key}")
        df2 = df2.dropDuplicates(["url"])
        df2 = df2.withColumn("comments", col("comments").cast("string"))
        if "modified_at" in df2.columns:
            df2 = df2.drop("modified_at")
        df = df.union(df2)

    timestamp = keys[0].split("/")[-1].split(".")[0].split("_")[-1].replace("T", " ")

    print(f"timestamp (file_create_time): {timestamp}")
    print(f"data_source union df {df}")

    return df, timestamp


def transform(spark, df, timestamp):

    raw_df, view_raw_data_df, current_issue_df, frequency_df = (
        get_current_issue_and_raw_data(spark, df, timestamp)
    )

    similar_df = get_similarity(spark, current_issue_df)

    return raw_df, view_raw_data_df, current_issue_df, frequency_df, similar_df


def load(raw_data_df, view_raw_data_df, current_issue_df, frequency_df, similar_df):
    load_to_redshift(raw_data_df, "raw_data_df")
    load_to_redshift(view_raw_data_df, "view_raw_data_df")

    delete_issue_graph_view_and_keyword_frequency_view()
    load_to_redshift(current_issue_df, "current_issue_df")
    load_to_redshift(frequency_df, "frequency_df")
    create_issue_graph_view_and_keyword_frequency_view()

    load_to_redshift(similar_df, "similar_df")


def alert_alarm(frequency_df, similar_df):

    alarm_df = frequency_df.join(similar_df, on="current_keyword", how="left")

    alarm_df = alarm_df.withColumn(
        "alert_value", col("similarity_degree") * col("total_frequency")
    )

    alarm_df = alarm_df.orderBy(col("alert_value").desc()).first()

    keyword = alarm_df["current_keyword"]
    score = alarm_df["alert_value"]

    message = {
        "AlarmName": f"코나 {keyword}",
        "NewStateValue": f"{score}",
        "NewStateReason": "{아이오닉6 누수}",
    }

    client = boto3.client("sns", region_name="ap-northeast-2")
    response = client.publish(
        TargetArn="arn:aws:sns:ap-northeast-2:367354627828:monitordog-notifications",
        Message=json.dumps(message),
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("emr").getOrCreate()
    df, timestamp = extract(spark)
    raw_df, view_raw_data_df, current_issue_df, frequency_df, similar_df = transform(
        spark, df, timestamp
    )
    print("transform finish")

    load(raw_df, view_raw_data_df, current_issue_df, frequency_df, similar_df)
    alert_alarm(frequency_df, similar_df)
