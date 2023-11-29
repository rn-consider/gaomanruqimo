import csv

import pandas as pd
import random
from datetime import datetime
import hashlib

from pyspark.sql import SparkSession

from movieLearn import recommend_movies_for_user


# 读取用户数据
# Function to load user data
def load_user_data(username):
    try:
        user_data = pd.read_csv("data/users.csv", encoding="utf-8")
        if not user_data.empty and 'USER_NICKNAME' in user_data.columns:
            user_data = user_data[user_data['USER_NICKNAME'] == username]
        else:
            user_data = pd.DataFrame(columns=["USER_ID", "USER_NICKNAME"])
        return user_data
    except FileNotFoundError:
        return pd.DataFrame(columns=["USER_ID", "USER_NICKNAME"])


# 保存用户数据
def save_user_data(user_data, output_file="data/users.csv"):
    """
    将用户数据追加到指定的 CSV 文件中。

    Parameters:
    - user_data (pd.DataFrame): 包含用户数据的 DataFrame。
    - output_file (str): 输出的 CSV 文件路径，默认为 "data/user.csv"。

    Returns:
    - None
    """
    user_data.to_csv(output_file, mode='a', header=False, index=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)


# 保存评分数据
def save_rating_data(rating_data, output_file="data/ratings.csv"):
    rating_data.to_csv(output_file, mode='a', header=False, index=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)


# 获取当前时间
def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def random_movie(movies, num_movies=999):
    """
    从电影数据中随机选择一定数量的电影。

    Parameters:
    - movies (pd.DataFrame): 包含电影数据的 DataFrame。
    - num_movies (int): 选择的电影数量，默认为 3。

    Returns:
    - pd.DataFrame: 包含随机选择的电影信息的 DataFrame。
    """
    return movies.sample(num_movies)


# CUI程序
def cui_recommendation():
    print("欢迎使用电影推荐系统！")

    # 获取用户输入的用户名
    username = input("请输入您的用户名: ")

    # 加载用户数据
    user_data = load_user_data(username)

    if user_data.empty:
        print(f"欢迎新用户 {username}！")
        user_data = pd.DataFrame({
            'USER_ID': len(pd.read_csv(
                'data/users.csv',
            )) +1,
            'USER_NICKNAME': [username]
        })
        # 保存用户数据
        save_user_data(user_data)
    else:
        print(f"欢迎回来，{username}！")

    # 提供用户选择操作的选项
    print("\n请选择操作:")
    print("1. 获取推荐电影（10部）")
    print("2. 进行电影评分")

    # 获取用户选择
    choice = input("请输入选项 (1 or 2): ")

    if choice == "1":
        spark = SparkSession.builder.appName("MovieLens Recommendation").getOrCreate()

        # 读取电影数据
        movies = pd.read_csv("data/movies.csv", encoding="utf-8")
        recommended_movies = recommend_movies_for_user(spark, "./data", int(user_data['USER_ID'].values[0]))
        print("\n推荐电影：")
        recommended_movies.show()

    elif choice == "2":
        # 读取电影数据
        movies = pd.read_csv("data/movies.csv", encoding="utf-8")

        # 随机抽取电影
        random_movies = random_movie(movies)
        print("\n随机抽取的电影：")
        print(random_movies)

        # 用户输入评分
        for _, movie in random_movies.iterrows():
            while True:
                try:
                    print(f"\n电影 '{movie['NAME']}' 的简介是 {movie['STORYLINE']} : ")
                    rating = int(input(f"\n请为电影 '{movie['NAME']}' 打分 (1-5): "))
                    if 1 <= rating <= 5:
                        break
                    else:
                        print("评分应在1到5之间，请重新输入。")
                except ValueError:
                    print("请输入有效的数字。")
                except KeyboardInterrupt:
                    print("\n您已退出程序。")
                    exit(0)

            # 在用户数据中记录评分
            rating_data = pd.DataFrame({
                'RATING_ID': [str(int(datetime.now().timestamp()))],
                'USER_ID': [user_data['USER_ID'].values[0]],
                'MOVIE_ID': [movie['MOVIE_ID']],
                'RATING': [rating],
                'RATING_TIME': [get_current_time()]
            })

            # 保存评分数据
            save_rating_data(rating_data)
            print(f"已记录您对电影 '{movie['NAME']}' 的评分：{rating}")

        print("\n感谢您的参与！")
    else:
        print("无效的选项。请重新运行程序并输入有效选项。")


if __name__ == "__main__":
    cui_recommendation()
