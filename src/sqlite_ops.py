import sqlite3
import logging

import config


DB_PATH = config.DB_PATH
LOGGER = logging.getLogger(__name__)


def get_cheaters():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cheaters")
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        # return rows
        return [dict(zip(colnames, row)) for row in rows]

    except Exception as e:
        LOGGER.error(f"Error occured when you tried to get cheaters: {e}")
        return None


def insert_data(players):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executemany("INSERT INTO updated_players VALUES(?,?,?,?,?,?)", players)
        conn.commit()
        cursor.close()

        return True

    except Exception as e:
        LOGGER.error(f"Error occured when you tried to get cheaters: {e}")
        return None
    