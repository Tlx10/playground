from gnews import GNews
import os
import json
import pandas as pd
import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta


class IRMProcessor:
    def __init__(self, staging_folder, partitioned_folder, features_path, categories, output_dir):
        self.staging_folder = staging_folder
        self.partitioned_folder = partitioned_folder
        self.features_path = features_path
        self.categories = categories
        self.output_dir = output_dir

        # Read the features.csv file
        features_df = pd.read_csv(self.features_path)

        # Convert columns to lists
        self.company_name_list = features_df['keyword'].tolist()

    def clean_directory_name(self, name):
        return name.replace('https://', '').replace('http://', '').replace('/', '_').replace(':', '_')

    def generate_hash(self, url):
        return hashlib.md5(url.encode()).hexdigest()

    def saveFile(self, file_content, folder_name, file_name, company_name):
        file_content['timestamp'] = datetime.utcnow().isoformat() + 'Z'

        if 'url' in file_content:
            file_content['hash_url'] = self.generate_hash(file_content['url'])

        safe_file_name = "".join([c for c in file_name if c.isalpha() or c.isdigit() or c in ' ._-']).rstrip()
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        file_path = os.path.join(folder_name, safe_file_name + ".json")

        if 'published date' in file_content:
            file_content['published_date'] = file_content.pop('published date')

        if 'title' in file_content and 'content' in file_content:
            file_content['text'] = f"{file_content['title']}\n\n{file_content['content']}"

        file_content['keyword'] = company_name

        with open(file_path, 'w') as file:
            json.dump(file_content, file, indent=4)

    def increment_date(self, year, month, day):
        if month == 12 and day == 31:
            return (year + 1, 1, 1)
        elif day == 31 and month in [1, 3, 5, 7, 8, 10]:
            return (year, month + 1, 1)
        elif day == 30 and month in [4, 6, 9, 11]:
            return (year, month + 1, 1)
        elif day == 28 and month == 2:
            return (year, 3, 1)
        elif day == 29 and month == 2:
            return (year, 3, 1)
        else:
            return (year, month, day + 1)

    def getNews_by_type(self, news_type, queries, start_date, end_date):
        google_news = GNews()
        google_news.max_results = 100

        if not isinstance(queries, list):
            queries = [queries]

        current_year, current_month, current_day = start_date
        end_year, end_month, end_day = end_date

        def fetch_and_save(query, i, year, month, day):
            clean_query = self.clean_directory_name(query)
            folder_path = os.path.join(self.staging_folder, news_type, clean_query)

            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            data_folder_path = os.path.join(folder_path, f"{year}", f"{month:02d}", f"{day:02d}")
            if not os.path.exists(data_folder_path):
                os.makedirs(data_folder_path)

            # Setting the date range for the query
            google_news.start_date = (year, month, day)
            google_news.end_date = (year, month, day)

            if news_type in ['Keywords']:
                json_resp = google_news.get_news(query)
            else:
                return

            for link in range(len(json_resp)):
                try:
                    article = google_news.get_full_article(json_resp[link]['url'])
                    json_resp[link]['content'] = article.text
                    title = json_resp[link].get('title', 'No Title')[:50]
                    self.saveFile(json_resp[link], data_folder_path, title, self.company_name_list[i])
                except Exception as e:
                    print(f"Failed to obtain content for link. Skipped... Error: {e}")

        while (current_year, current_month, current_day) <= (end_year, end_month, end_day):
            with ThreadPoolExecutor(max_workers=1) as executor:
                futures = []
                for i, query in enumerate(queries):
                    futures.append(executor.submit(fetch_and_save, query, i, current_year, current_month, current_day))
                for future in futures:
                    future.result()
            current_year, current_month, current_day = self.increment_date(current_year, current_month, current_day)

    def json_to_csv(self, folder_path, output_folder, category_name):
        all_data = []

        for company_folder in os.listdir(folder_path):
            company_path = os.path.join(folder_path, company_folder)
            if os.path.isdir(company_path):
                for year_folder in os.listdir(company_path):
                    year_path = os.path.join(company_path, year_folder)
                    if os.path.isdir(year_path):
                        for month_folder in os.listdir(year_path):
                            month_path = os.path.join(year_path, month_folder)
                            if os.path.isdir(month_path):
                                for day_folder in os.listdir(month_path):
                                    day_path = os.path.join(month_path, day_folder)
                                    if os.path.isdir(day_path):
                                        for filename in os.listdir(day_path):
                                            full_path = os.path.join(day_path, filename)
                                            if os.path.isfile(full_path) and filename.endswith('.json'):
                                                with open(full_path, 'r') as file:
                                                    try:
                                                        data = json.load(file)
                                                        all_data.append(data)
                                                    except json.JSONDecodeError as e:
                                                        print(f"Failed to decode JSON from file {full_path}: {e}")

        if not all_data:
            print(f"No data found in category {category_name}.")
            return

        df = pd.DataFrame(all_data)

        os.makedirs(output_folder, exist_ok=True)
        csv_path = os.path.join(output_folder, f'{category_name}.csv')
        df.to_csv(csv_path, index=False)
        print(f"Data compiled into {csv_path}")

    def convert_json_to_csv(self):
        for category in self.categories:
            folder_path = os.path.join(self.staging_folder, category)
            if os.path.exists(folder_path):
                self.json_to_csv(folder_path, self.output_dir, category)
            else:
                print(f"Folder {folder_path} does not exist.")
