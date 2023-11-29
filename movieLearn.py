from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F


def recommend_movies_for_user(spark, data_path, target_user_id):
    # 读取电影和评分数据
    ratings = spark.read.csv(data_path + "/ratings.csv", header=True, inferSchema=True)

    # 使用ALS建立协同过滤模型
    als = ALS(userCol="USER_ID", itemCol="MOVIE_ID", ratingCol="RATING", coldStartStrategy="drop")
    model = als.fit(ratings)

    # 为指定用户生成推荐结果
    target_user_df = spark.createDataFrame([Row(USER_ID=target_user_id)])
    userRecs = model.recommendForUserSubset(target_user_df, 10)

    # 将推荐结果与电影数据集连接，添加电影名列
    movies = spark.read.csv(data_path + "/movies.csv", header=True, inferSchema=True)
    userRecs = userRecs.withColumn("recommendations", F.expr("explode(recommendations)")) \
        .select("USER_ID", "recommendations.*")

    # 添加 Movie_Name 列
    userRecs = userRecs.join(movies.select("MOVIE_ID", "NAME"), on="MOVIE_ID")

    # 重命名列
    userRecs = userRecs.select("USER_ID", "MOVIE_ID", "NAME", "rating").withColumnRenamed("NAME", "Movie_Name")

    return userRecs


if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieLens Recommendation").getOrCreate()

    # 数据路径需要根据实际情况调整
    data_path = "./data"

    # 指定目标用户ID
    target_user_id = 2  # 请替换为实际的用户ID

    result_df = recommend_movies_for_user(spark, data_path, target_user_id)

    # 打印结果
    print("Recommendations for User:")
    result_df.show()

    spark.stop()
