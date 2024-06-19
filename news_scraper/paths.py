import os

staging_folder = os.path.expanduser('/Users/lixin/Desktop/playground')
partitioned_folder = os.path.expanduser('/Users/lixin/Desktop/playground/gnews_partitioned')
features_path = '/Users/lixin/Desktop/playground/extract_topics/features.csv'
output_dir = os.path.expanduser('/Users/lixin/Desktop/playground/my_website')
csv_file_path = '/Users/lixin/Desktop/playground/scraped_data/news.csv'

# Database connection settings
username = 'smart_forecasting'
password = 'password'
host = '192.168.3.120'
port = '5432'
database = 'smart_forecasting'
schema = 'irm'

# Create engine URL using psycopg2
engine_url = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
