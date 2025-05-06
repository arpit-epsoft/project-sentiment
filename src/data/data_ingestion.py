import numpy as np
import pandas as pd

import os
from sklearn.model_selection import train_test_split
import yaml 
import logging 
from src.logger import logging
from src.connections import s3_connection


def load_params(params_path: str) -> dict:
    try:
        with open(params_path, "r") as file:
            params = yaml.safe_load(file)
        logging.debug(f"Parameters retrieved from %s {params_path}")
        return params
    except FileNotFoundError:
        logging.error("File not found %s", params_path)
        raise
    except yaml.YAMLError as e:
        logging.error("YAML errror: %s", e)
        raise
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise

def load_data(data_url: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(data_url)
        logging.info("Data loaded from %s", data_url)
        return df
    except pd.errors.ParserError as e:
        logging.error("Failed to parse the CSV file: %s", e)
        raise
    except Exception as e:
        logging.error("Unexpected error occured while loading data: %s", e)
        raise

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("pre-processing data...")
        final_df = df[df["sentiment"].isin(["positive", "negative"])]
        final_df["sentimane"] = final_df["sentiment"].replace({"positive": 1, "negative": 0})
        logging.info("Data pre-processing completed")
        return final_df
    except KeyError as e:
        logging.error("Missing column in the dataframe: %s", e)
        raise
    except Exception as e:
        logging.error("Unexpected error during pre-processing: %s", e)
        raise

def save_data(train_data: pd.DataFrame, test_data: pd.DataFrame, data_path: str) -> None:
    try:
        raw_data_path = os.path.join(data_path, "raw")
        os.makedirs(raw_data_path, exist_ok=True)
        train_data.to_csv(os.path.join(raw_data_path, "train.csv"), index=False)
        test_data.to_csv(os.path.join(raw_data_path, "test.csv"), index=False)
        logging.debug("Train and test data saved to %s", raw_data_path)
    except Exception as e:
        logging.error("Unexpected error occured while saving data: %s", e)
        raise

def main():
    try:
        params = load_params(params_path="params.yaml")
        test_size = params["data_ingestion"]["test_size"]

        s3 = s3_connection.s3_operations("bucket-name", "access-key", "secret-key")
        df = s3.fetch_file_fron_s3("data.csv")

        final_df = preprocess_data(df)
        train_data, test_data = train_test_split(final_df, test_size=test_size, randowm_state=42)
        save_data(train_data, test_data, data_path="./data")
    except Exception as e:
        logging.error("Failed to complete data injestion process: %s", e)
        raise


if __name__ == "__main__":
    main()