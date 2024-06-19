import pandas as pd
from gnews import GNews
gn = GNews()
gn.start_date = (2024,6,17)
gn.end_date = (2024,6,18)
news = gn.get_news('semiconductor industry')
df = pd.DataFrame(news)
print(df.shape)