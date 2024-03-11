import subprocess
import time
from datetime import datetime
from pathlib import Path

import psutil

from dbfread.dbf import DBF
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from meteoconfig import *


# InfluxDB timezone is always UTC
utc_zone = tz.tzutc()


def get_last_timestamp(influx_client) -> datetime | None:
    """
    Get last timestamp from InfluxDB
    :param influx_client: initialized InfluxDB client
    :return: last timestamp or None if no data are present in InfluxDB
    """
    query = f'from(bucket: "{influxdb_bucket}")' \
            f'  |> range(start: -1y)' \
            f'  |> filter(fn: (r) => r["_measurement"] == "{influxdb_measurement}")' \
            f'  |> last()'
    tables = influx_client.query_api().query(query, org=influxdb_org)
    if tables:
        for table in tables:
            for record in table.records:
                last_time = record.get_time().replace(tzinfo=utc_zone)
                return last_time.astimezone(local_zone)
    return None


def write_to_influxdb(influx_write_api, data) -> None:
    """
    Writes data to InfluxDB
    :param influx_write_api: write API object of initialized InfluxDB client
    :param data: list of dictionaries where each dictionary represents one row from DBF file
    :return: None
    """
    print("Parsing into InfluxDB points...")
    points = []

    for row in data:
        point = Point(influxdb_measurement)
        for key, value in row.items():
            match key:
                case "DAT" | "CAS" | "EX" | "RESETCNT" | "RESETTYP" | "_NullFlags":
                    continue  # system columns
                case "PWD_ERR" | "PWD_V01" | "PWD_V10" | "PWD_P01" | "PWD_P15" | \
                     "PWD_WI" | "PWD_WS" | "PWD_SS" | "PWD_T" | "PWD_BL":
                    continue  # currently not used columns
                case "VLVZD":
                    name = "humi"
                case "TEP2M":
                    name = "temp"
                case "TEP2M_I":
                    name = "temp_min"
                case "TEP2M_X":
                    name = "temp_max"
                case "TLAK":
                    name = "press"
                case "TLAK_M":
                    name = "press_sea"
                case "SRAZKY":
                    name = "rain"
                case "RYCHV":
                    name = "wind_speed"
                case "SMERV":
                    name = "wind_dir"
                case "RYCHV_P":
                    name = "wind_speed_avg"
                case "SMERV_P":
                    name = "wind_dir_avg"
                case "RYCHV_X":
                    name = "wind_speed_max"
                case "SMERV_X":
                    name = "wind_dir_max"
                case "CASV_X":
                    name = "wind_time_max"
                case "NABAT_E":
                    name = "volt_exp"
                case "NABAT":
                    name = "volt"
                case "NABAT_I":
                    name = "volt_min"
                case _:
                    print(f"ERROR: Unknown column: {key}")
                    continue

            point.field(name, float(value))
        timestamp = datetime.strptime(f"{row['DAT']} {row['CAS']}", '%Y-%m-%d %H:%M').replace(tzinfo=local_zone)
        point.time(timestamp.astimezone(utc_zone), WritePrecision.NS)
        points.append(point)

    print("Writing to InfluxDB...")
    influx_write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=points)


def read_new_rows_from_dbf(last_timestamp) -> list[dict]:
    """
    Reads new rows from DBF file and returns them as a list of dictionaries
    :param last_timestamp: only rows with timestamp greater than this will be returned
    :return: list of dictionaries where each dictionary represents one row from DBF file
    """
    table = DBF(dbf_path, load=True)
    new_rows = []
    for record in table:
        record_time = datetime.strptime(f"{record['DAT']} {record['CAS']}", '%Y-%m-%d %H:%M').replace(tzinfo=local_zone)
        if record_time > last_timestamp:
            new_rows.append(record)
    return new_rows


def start_winmeteo_if_not_running(process_name, process_path, working_dir) -> None:
    """
    Starts WinMeteo process if it is not running
    :param process_name: name of the executable
    :param process_path: path to the executable
    :param working_dir: working directory for the process
    :return: None
    """
    if not any(proc.name() == process_name for proc in psutil.process_iter()):
        subprocess.Popen(process_path, cwd=working_dir)
        print(f"Started {process_name}.")


def kill_winmeteo(process_name, kill_time_limit) -> None:
    """
    Kills WinMeteo process
    :param process_name: name of the executable
    :param kill_time_limit: maximum time in secods to wait for the process to terminate
    :return: None
    """
    # Attempt to kill the process
    process_exists = any(proc.name() == process_name for proc in psutil.process_iter())
    if process_exists:
        try:
            subprocess.run(["taskkill", "/f", "/im", process_name], check=True)
            print(f"Attempted to kill {process_name}.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to terminate {process_name}. Error: {e}")
            return
    else:
        print(f"Process {process_name} is not running.")
        return

    # Check if the process has been killed, with a timeout
    start_time = time.time()
    while True:
        process_exists = any(proc.name() == process_name for proc in psutil.process_iter())
        if not process_exists:
            print(f"Process {process_name} has been terminated successfully.")
            break

        if time.time() - start_time > kill_time_limit:
            raise OSError(f"Failed to terminate process {process_name} within {kill_time_limit} seconds.")

        time.sleep(1)


def main():
    winmeteo_path_obj = Path(winmeteo_path)
    winmeteo_dir = winmeteo_path_obj.parent
    winmeteo_process = winmeteo_path_obj.name

    start_winmeteo_if_not_running(winmeteo_process, winmeteo_path, winmeteo_dir)

    connected = False
    last_timestamp = None
    write_api = None

    # Connect to InfluxDB
    while not connected:
        try:
            # Connect to InfluxDB
            client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
            write_api = client.write_api(write_options=SYNCHRONOUS)

            last_timestamp = get_last_timestamp(client) or datetime.min.replace(tzinfo=local_zone)
            print(f"Last timestamp in InfluxDB: {last_timestamp}")

            connected = True
        except Exception as e:  # noqa
            print(f"ERROR: {e}")
            print(f"Waiting for {error_timer} seconds before trying to connect again...")
            time.sleep(error_timer)

    iterations = 0
    zeros = 0
    restarts = 0
    last_restart = datetime.now()
    max_zeros = kill_timer // timer

    # Main loop
    while True:
        try:
            iterations += 1
            print("-" * 80)
            print(f"STATS: Iteration: {iterations}, restarts: {restarts}, last (re)start: {last_restart}")

            new_rows = read_new_rows_from_dbf(last_timestamp)
            print(f"Found {len(new_rows)} new rows.")

            if new_rows:
                write_to_influxdb(write_api, new_rows)
                last_timestamp = max(
                    datetime.strptime(f"{row['DAT']} {row['CAS']}", '%Y-%m-%d %H:%M').replace(tzinfo=local_zone)
                    for row in new_rows
                )
                print(f"Written {len(new_rows)} new rows to InfluxDB.")
                zeros = 0
            else:
                zeros += 1
                print(f"No new rows found. Attempts: {zeros}/{max_zeros}")

            if zeros >= max_zeros:
                print(f"Maximum attempts count reached. Restarting WinMeteo...")
                kill_winmeteo(winmeteo_process, wait_for_kill)
                start_winmeteo_if_not_running(winmeteo_process, winmeteo_path, winmeteo_dir)
                restarts += 1
                last_restart = datetime.now()
                zeros = 0
        except Exception as e: # noqa
            print(f"ERROR: {e}")
            print(f"Waiting for {error_timer} seconds before trying again...")
            time.sleep(error_timer)
        else:
            print(f"Sleeping for {timer} seconds...")
            time.sleep(timer)  # Wait for X seconds before checking again


if __name__ == '__main__':
    main()
