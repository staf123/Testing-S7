import csv
import json
import os
import pandas as pd
import time
from loguru import logger
import sqlite3



def watch_file(filename, time_limit=3600, check_interval=60):

    now = time.time()
    last_time = now + time_limit

    while time.time() <= last_time:
        if os.path.exists(filename):
            return True
        else:
            time.sleep(check_interval)
    return False


if __name__ == "__main__":
    filename = "20201129_1234_DME.csv"
    time_limit = 60
    check_interval = 1

    if watch_file(filename, time_limit, check_interval):
        print(f"File created: {os.path.abspath(filename)}")
    else:
        print(
            f"File {filename} not found after waiting: {time_limit} seconds!"
        )

logger.add("Info.log", format="{time} {level} {message}", level="INFO", rotation="1 week", compression="zip" )

logger.info("successful, (info)!")

try:
    df = pd.read_csv (r"C:\Users\Xiaomi\PycharmProjects\pythonProject\In\20201129_1234_DME.csv")
    df.to_json (r"C:\Users\Xiaomi\PycharmProjects\pythonProject\Out\20201129_1234_DME.json")

except:
    df.to_json (r"C:\Users\Xiaomi\PycharmProjects\pythonProject\Err\20201129_1234_DME.json")

logger.add("Info.log", format="{time} {level} {message}", level="INFO", rotation="1 week", compression="zip" )

logger.info("successful, (info)!")


conn = sqlite3.connect('S7.db')
cur = conn.cursor()

cur.execute(""" CREATE TABLE IF NOT EXISTS Flight(
    id INT PRIMARY KEY AUTOINCREMENT,
    file_name TEXT NOT NULL,
    flt INT NOT NULL,
    depdate DATE NOT NULL,
    dep TEXT NOT NULL);
""")
conn.commit()

cur.execute("""INSERT INTO Flight(file_name, flt, depdate, dep)
   VALUES("20201129_1234_DME", 1234, 20201129, 'DME');""")
conn.commit()

logger.add("Info.log", format="{time} {level} {message}", level="INFO", rotation="1 week", compression="zip" )

logger.info("successful, (info)!")
