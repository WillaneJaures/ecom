import pandas as pd
import logging
import os
from datetime import datetime


DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")
CLEAN_DATA_DIR = os.path.join(DATA_DIR, "clean_data")
ENRICHED_DATA_DIR = os.path.join(DATA_DIR, "enriched_data")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



########################################
# Enrichment functions
########################################
def read_clients(date: datetime):
    try:
        clients_df = pd.read_csv(f"data/clean_data/clients/{date.year}/{date.month}/{date.day}.csv")
        return clients_df
    except FileNotFoundError:
        logging.error("File not found")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
    return pd.DataFrame()  # Return an empty DataFrame if there's an error


def read_orders(date: datetime):
    try:
        orders_df = pd.read_csv(f"data/clean_data/orders/{date.year}/{date.month}/{date.day}.csv")
        return orders_df
    except FileNotFoundError:
        logging.error("File not found")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
    return pd.DataFrame()  # Return an empty DataFrame if there's an error


def read_products(date: datetime):
    try:
        products_df = pd.read_csv(f"data/clean_data/products/{date.year}/{date.month}/{date.day}.csv")
        return products_df
    except FileNotFoundError:
        logging.error("File not found")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
    return pd.DataFrame()  # Return an empty DataFrame if there's an error


def enrich_stock(date: datetime):
    logging.info("Enriching stock data")


    product_df = read_products(date)
    order_df = read_orders(date)

    # Harmoniser colonnes
    order_df = order_df.rename(columns={"order_date": "date"})

    # Convertir dates
    product_df["date"] = pd.to_datetime(product_df["date"])
    order_df["date"] = pd.to_datetime(order_df["date"])


    daily_sales = order_df.groupby(["date", "product_id"])["quantity"].sum().reset_index()

    df = pd.merge(product_df, daily_sales, how="left", left_on=["date", "product_id"], right_on=["date", "product_id"])
    df["quantity"] = df["quantity"].fillna(0)

    print(df.head(5))

    df["stock_remaining"] = df["stock"] - df["quantity"]

    os.makedirs(f"{ENRICHED_DATA_DIR}/stock/{date.year}/{date.month}", exist_ok=True)
    local_path = os.path.join(f"{ENRICHED_DATA_DIR}/stock/{date.year}/{date.month}", f"{date.day}.csv")
    if df.shape[0]>0:
        df.to_csv(local_path, index=False)
    logging.info(f"Enriched stock data saved to {local_path}")



if __name__ == "__main__":
    enrich_stock(datetime.strptime("2024-05-03", "%Y-%m-%d"))

    


    