#!/usr/bin/env python3

import requests, json, logging, time, psycopg2, signal, sys
from psycopg2.extras import execute_values

CHUNK_SIZE = 96
INTERVAL = 10  # fetch interval in seconds
LOG_LEVEL = logging.ERROR
URL = "http://inverter/solar_api/v1/GetPowerFlowRealtimeData.fcgi"
HTTP_TIMEOUT = (
    10  # without a timeout a stalled inverter connection blocks the loop forever
)
CONNECT_TIMEOUT = 10
DB_CONNINFO = (
    "dbname='home' user='electricity_stats_collector' connect_timeout=%d"
    % CONNECT_TIMEOUT
)

logging.basicConfig(level=LOG_LEVEL)

shutting_down = False


def connect_db():
    while True:
        try:
            return psycopg2.connect(DB_CONNINFO)
        except psycopg2.Error as e:
            logging.error("db not ready: %s", e)
            if shutting_down:
                raise
            time.sleep(5)


db_conn = connect_db()
db_cursor = db_conn.cursor()
chunk = []


def fetch():
    resp = requests.get(url=URL, timeout=HTTP_TIMEOUT).json()

    sample = {
        "time": int(time.time()),
        "prod": resp["Body"]["Data"]["Site"]["P_PV"] or 0,
        "load": round(resp["Body"]["Data"]["Site"]["P_Load"]) * -1,
        # "grid": round(resp['Body']['Data']['Site']['P_Grid'])
    }
    logging.debug("sample: %s", sample)
    chunk.append(sample)


def insert(max_attempts=0):
    # max_attempts=0: retry forever (normal path, no data loss while DB is briefly down)
    # max_attempts=1: single attempt (shutdown path, never hang the exit)
    global db_conn, db_cursor
    columns = chunk[0].keys()
    values_list = [tuple([sample[c] for c in columns]) for sample in chunk]
    attempts = 0
    while True:
        attempts += 1
        try:
            execute_values(
                db_cursor,
                f"INSERT INTO electricity ({','.join(columns)}) VALUES %s",
                values_list,
            )
            db_conn.commit()
            logging.info("inserted %d samples", len(values_list))
            return
        except Exception as e:
            logging.error("insert failed: %s", e)
            try:
                db_conn.close()
            except Exception:
                pass
            if (max_attempts and attempts >= max_attempts) or shutting_down:
                raise
            db_conn = connect_db()
            db_cursor = db_conn.cursor()


def exit_handler(signal_received, frame):
    global shutting_down
    if shutting_down:
        return
    shutting_down = True
    logging.warning(
        "signal %s received, flushing %d buffered samples", signal_received, len(chunk)
    )
    try:
        if chunk:
            insert(max_attempts=1)
    except Exception as e:
        logging.error("flush on exit failed: %s", e)
    try:
        db_conn.close()
    except Exception:
        pass
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    while True:
        try:
            fetch()
        except Exception as e:
            logging.error(f"{__file__}: Error fetching data\n{e}")

        if len(chunk) >= CHUNK_SIZE:
            try:
                insert()
            except Exception as e:
                logging.error("insert failed: %s", e)
            else:
                chunk = []

        time.sleep(INTERVAL)
