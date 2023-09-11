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


def check_if_cheater(user_id, server_timestamp):
    cheaters = sqlite_ops.get_cheaters()
    if cheaters is None:
        return None
    
    for cheater in cheaters:
        cheater_id = cheater.get("player_id")
        if cheater_id is None:
            continue

        if cheater_id != user_id:
            continue
            
        ban_time = cheater.get("ban_time")
        if ban_time is None:
            continue

        ban_time = datetime.datetime.strptime(ban_time, "%Y-%m-%d %H:%M:%S")
            
        user_date = datetime.datetime.utcfromtimestamp(server_timestamp)
        user_date = user_date + datetime.timedelta(hours=3)

        # print(f"user_date: {user_date} - banned_date: {ban_time} - delta: {user_date - ban_time}")
        if user_date - ban_time >= datetime.timedelta(days=1):
            return False
    
    return True


def remove_cheaters_convert2list(data_pd):
    output = []

    cheaters = sqlite_ops.get_cheaters()
    if cheaters is None:
        return None

    for log in data_pd.iterrows():
        user_data = log[1]        
        user_id = user_data.iloc[5]
        server_timestamp = user_data.iloc[0]
        if check_if_cheater(user_id, server_timestamp) is not True:
            continue

        event_id = user_data.iloc[1]
        error_id = user_data.iloc[2]
        json_server = user_data.iloc[3]
        json_client = user_data.iloc[6]

        output.append((server_timestamp, user_id, event_id, error_id, json_server, json_client))

    return output
        

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

    joined_data = remove_cheaters_convert2list(joined_data)
    if joined_data is None:
        return None
    
    if sqlite_ops.insert_data(joined_data) is None:
        return None
    
    return True

