import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.font_manager import FontProperties

# 设置中文字体
font_path = '/home/consider/项目/高曼如期末/SourceHanSansSC-Bold.otf'  # 替换为你的字体文件路径
myfont = FontProperties(fname=font_path)

# 读取 movies.csv 和 ratings.csv 文件
movies_df = pd.read_csv('data/movies.csv', encoding='utf-8')
ratings_df = pd.read_csv('data/ratings.csv', encoding='utf-8')

# 合并两个数据框基于 MOVIE_ID
merged_df = pd.merge(ratings_df, movies_df, on='MOVIE_ID')

# 计算每部电影的平均评分，只考虑有评分数据的电影
average_ratings = merged_df.groupby('NAME')['RATING'].mean().reset_index()

# 过滤掉没有评分数据的电影
average_ratings = average_ratings.dropna()

# 排序电影按照平均评分
average_ratings = average_ratings.sort_values(by='RATING', ascending=False)

# 可视化
fig, ax = plt.subplots(figsize=(12, 8))
bars = ax.barh(average_ratings['NAME'], average_ratings['RATING'], color='skyblue')
ax.set_xlabel('平均评价', fontproperties=myfont)
ax.set_title('每个电影的平均分', fontproperties=myfont)

# 设置坐标轴刻度的字体
for tick in ax.get_xticklabels() + ax.get_yticklabels():
    tick.set_fontproperties(myfont)

ax.invert_yaxis()  # 反转y轴，让评分高的电影在上面
plt.show()
