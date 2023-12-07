import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.font_manager import FontProperties

font_path = 'SourceHanSansSC-Bold.otf'  # 替换为你的字体文件路径
myfont = FontProperties(fname=font_path)

# 读取 movies.csv 文件
movies_df = pd.read_csv('data/movies.csv', encoding='utf-8')

# 统计每个年份的电影数量
#movie_counts_by_year = movies_df['YEAR'].value_counts().sort_index()
movie_counts_by_year = movies_df[movies_df['YEAR'] <= 2023].value_counts().sort_index()
# 可视化
plt.figure(figsize=(12, 8))
plt.bar(movie_counts_by_year.index, movie_counts_by_year.values, color='skyblue')
plt.xlabel('年份',font=myfont)
plt.ylabel('电影数量',font=myfont)
plt.title('每年电影数量统计',font=myfont)
plt.show()
ort_index()

