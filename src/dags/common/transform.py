import pandas as pd
import logging
import os
from datetime import datetime
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")
CLEAN_DATA_DIR = os.path.join(DATA_DIR, "clean_data")

def transform_client(date: datetime):
    logging.info("Transforming clients data")
    #read raw data of the day
    try:
        df = pd.read_csv(f"data/raw_data/clients/{date.year}/{date.month}/{date.day}.csv")
        #print(df.head(5))
        
        os.makedirs(f"{CLEAN_DATA_DIR}/clients/{date.year}/{date.month}", exist_ok=True)
        local_path = os.path.join(f"{CLEAN_DATA_DIR}/clients/{date.year}/{date.month}", f"{date.day}.csv")
        #print(f"Saving transformed data to {local_path}")
        if df.shape[0]>0:
            df.to_csv(local_path, index=False)
        
        logging.info(f"Saving transformed data to {local_path}")
        
    except FileNotFoundError:
        logging.error("File not found")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def transform_orders(date: datetime):
    logging.info("Transforming orders data")
    #read raw data of the day
    try:
        df = pd.read_csv(f"data/raw_data/orders/{date.year}/{date.month}/{date.day}.csv")
        #print(df.head(5))

        os.makedirs(f"{CLEAN_DATA_DIR}/orders/{date.year}/{date.month}", exist_ok=True)
        local_path = os.path.join(f"{CLEAN_DATA_DIR}/orders/{date.year}/{date.month}", f"{date.day}.csv")
        if df.shape[0]>0:
            df.to_csv(local_path, index=False)
        logging.info(f"Saving transformed data to {local_path}")

    except FileNotFoundError:
        logging.error("File not found")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def transform_products(date: datetime):
    logging.info("Transforming products data")
    #read raw data of the day
    try:
        df = pd.read_csv(f"data/raw_data/products/{date.year}/{date.month}/{date.day}.csv")
        logging.info(f"file read successfully")
        #print(df.head(5))

        os.makedirs(f"{CLEAN_DATA_DIR}/products/{date.year}/{date.month}", exist_ok=True)
        local_path = os.path.join(f"{CLEAN_DATA_DIR}/products/{date.year}/{date.month}", f"{date.day}.csv")
        if df.shape[0]>0:
            df.to_csv(local_path, index=False)
        
        logging.info(f"Saving transformed data to {local_path}")
    except FileNotFoundError:
        logging.error("File not found")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    #transform_client(datetime.strptime("2024-05-03", "%Y-%m-%d"))
    #transform_orders(datetime.strptime("2024-05-03", "%Y-%m-%d"))
    transform_products(datetime.strptime("2024-05-03", "%Y-%m-%d"))
