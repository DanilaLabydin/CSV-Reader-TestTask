import logging
import datetime
import time
import pandas as pd

import config
import sqlite_ops


LOGGER = logging.getLogger(__name__)
SERVER_PATH = config.SERVER_PATH
CLIENT_PATH = config.CLIENT_PATH
SERVER_COLUMNS = config.SERVER_COLUMNS
CLIENT_COLUMNS = config.CLIENT_COLUMNS


def read_csv_data(file_path):
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        LOGGER.error(
            f"Error occured when you tried to read data from {file_path} file: {e}"
        )
        return None


def filter_data_by_date(data_pd, date):
    start_date = datetime.datetime.strptime(date + " 00:00:00", "%d-%m-%Y %H:%M:%S")
    end_date = datetime.datetime.strptime(date + " 23:59:59", "%d-%m-%Y %H:%M:%S")

    start_timestamp = time.mktime(start_date.timetuple())
    end_timestamp = time.mktime(end_date.timetuple())

    try:
        return data_pd[
            (data_pd["timestamp"] >= start_timestamp)
            & (data_pd["timestamp"] <= end_timestamp)
        ]

    except Exception as e:
        LOGGER.error(
            f"Error occured when you tried to filter initial data by date: {e}"
        )
        return None


def remove_cheaters(data_pd):
    user_id_cashe = []

    cheaters = sqlite_ops.get_cheaters()
    if cheaters is None:
        return None

    for log in data_pd.iterrows():
        user_id = log[1][5]
        if user_id in user_id_cashe:
            continue


def insert_game_data(date="12-04-2021"):
    server_data = read_csv_data(SERVER_PATH)
    client_data = read_csv_data(CLIENT_PATH)

    if server_data is None or client_data is None:
        return None

    if list(server_data.columns.values) != SERVER_COLUMNS:
        LOGGER.error(
            f"Columns from {SERVER_PATH} not match with set columns from config.py"
        )
        return None

    if list(client_data.columns.values) != CLIENT_COLUMNS:
        LOGGER.error(
            f"Columns from {CLIENT_PATH} not match with set columns from config.py"
        )
        return None

    server_data = filter_data_by_date(server_data, date)
    client_data = filter_data_by_date(client_data, date)

    if server_data is None or client_data is None:
        return None

    joined_data = pd.merge(server_data, client_data, on="error_id", how="inner")

    joined_data = remove_cheaters(joined_data)
    if joined_data is None:
        return None


# user_date = input("Enter the date in dd-mm-yyyy format (like 12-04-2021): ")


insert_game_data()
# input user date
# create date range (user_date 00:00:00 AM - user_date 11:59:59 PM )
# convert date range to timestamp
# filter date1, date2 by timestamp range
# join date1, date2 by error_id


# reading two csv files
