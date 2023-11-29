import pandas as pd

def reindex_users_and_update_ratings(user_file, rating_file):
    # 读取用户数据
    users = pd.read_csv(user_file)

    # 添加新的编号列
    users['USER_ID'] = range(1, len(users) + 1)

    # 将 USER_ID 列的数据类型更改为整数
    users['USER_ID'] = users['USER_ID'].astype(int)

    # 创建编号字典
    user_id_dict = dict(zip(users['USER_MD5'], users['USER_ID']))

    # 更新评分数据
    ratings = pd.read_csv(rating_file)

    # 使用 map 方法映射 USER_MD5 到 USER_ID
    ratings['USER_MD5'] = ratings['USER_MD5'].map(user_id_dict)

    # 删除包含空值的行
    ratings = ratings.dropna(subset=['USER_MD5'])

    # 将 USER_MD5 列的数据类型更改为整数
    ratings['USER_MD5'] = ratings['USER_MD5'].astype(int)

    # 保存更新后的用户数据
    users[['USER_ID', 'USER_MD5', 'USER_NICKNAME']].to_csv(user_file, index=False)

    # 保存更新后的评分数据
    ratings.to_csv(rating_file, index=False)

# 数据路径需要根据实际情况调整
user_file_path = "./data/users.csv"
rating_file_path = "./data/ratings.csv"

reindex_users_and_update_ratings(user_file_path, rating_file_path)
