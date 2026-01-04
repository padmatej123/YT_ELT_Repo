#old
import json
from datetime import date
import logging

logger=logging.getLogger(__name__)

def load_data():
    file_path = f"./data/YT_data_{date.today()}.json"

    try:
        logger.info(f"Processing file: YT_data_{date.today()}")

        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)

        return data

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format in file: {file_path}")
        raise



#new    
# import json
# from datetime import date
# import logging
# import os

# logger = logging.getLogger(__name__)

# def load_data():
#     BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#     file_path = os.path.join(
#         BASE_DIR, "..", "..", "data", f"YT_data_{date.today()}.json"
#     )

#     try:
#         logger.info(f"Processing file: {file_path}")

#         with open(file_path, "r", encoding="utf-8") as raw_data:
#             return json.load(raw_data)

#     except Exception as e:
#         logger.error(f"Failed loading data: {e}")
#         raise