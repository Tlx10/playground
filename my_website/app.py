from flask import Flask, render_template
import pandas as pd

app = Flask(__name__)

# 读取本地 CSV 文件
news_df = pd.read_csv('news.csv')

# 将 DataFrame 转换为字典列表
news_articles = news_df.to_dict(orient='records')

@app.route('/')
def index():
    return render_template('index.html', articles=news_articles)

@app.route('/article/<int:article_id>')
def article(article_id):
    article = news_articles[article_id]
    return render_template('article.html', article=article)

if __name__ == '__main__':
    app.run(debug=True)
