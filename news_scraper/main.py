import time
import os
from datetime import datetime
from gnews_processing import IRMProcessor
from paths import staging_folder, partitioned_folder, features_path, output_dir, csv_file_path, engine_url

# Initialize the IRMProcessor
categories = ['Keywords']
processor = IRMProcessor(staging_folder, partitioned_folder, features_path, categories, output_dir)

# Define the date range for news collection
start_date = (2024,6,17)
end_date = (2024,6,18)

processor.getNews_by_type('Keywords', processor.company_name_list, start_date, end_date)
processor.convert_json_to_csv()
# processor.push_db()
