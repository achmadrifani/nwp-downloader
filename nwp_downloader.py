#!/home/metpublic/PYTHON_VENV/present_weather/bin/python

import yaml
from datetime import datetime, timedelta
import requests
from requests.exceptions import Timeout, RequestException, HTTPError, ConnectionError
import os
import argparse
import pickle

ROOT_DIR = "/home/metpublic"
TASK_DIR = "PYTHON_SCRIPT/nwp_downloader"
DATA_REPOS = f"DATA_REPOS"

# CONFIG_DIR = f"{ROOT_DIR}/{TASK_DIR}/config"
# CONFIG_DIR = f"D:/Projects/nwp_downloader/config"


def read_cips_config():
    file = f"{CONFIG_DIR}/CIPS_CONFIG.yml"
    with open(file, 'r') as f:
        cfg = yaml.safe_load(f)
    return cfg


def read_model_config(file):
    config_file = f"{CONFIG_DIR}/{file}"
    with open(config_file, 'r') as f:
        cfg = yaml.safe_load(f)
    return cfg


def main(CONFIG_DIR):
    print("Reading configuration ...")
    cips_cfg = read_cips_config()
    cips = cips_cfg["CIPS1"]
    CIPS_HOST = cips["HOST"]
    CIPS_USER = cips["USER"]
    CIPS_PASS = cips["PASS"]

    mdl_cfg = read_model_config(CONFIG_DIR)
    INIT = mdl_cfg.get("INIT")
    MODEL = mdl_cfg.get("MODEL")
    GRID = mdl_cfg.get("GRID")
    PARAM_NAMES = mdl_cfg.get("PARAM_NAMES")
    STEPS = mdl_cfg.get("STEPS")
    STEPS = mdl_cfg.get("STEPS")

    # make steps range
    STEPS_HOLDER = []
    for STEP in STEPS:
        for _, value in STEP.items():
            start, stop, step_size = value
            STEPS_HOLDER += list(range(start, stop + step_size, step_size))
    print(STEPS_HOLDER)

    # check path
    print("Checking path ...")
    path = f"{ROOT_DIR}/{DATA_REPOS}/{MODEL}/{GRID}/{init_time}"
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Path {path} created.")
    else:
        print(f"Path {path} already exists.")

    # download data
    failed_file = []
    failed_url = []
    for param, param_info in PARAM_NAMES.items():
        for level in param_info['LEVELS']:
            for step in STEPS_HOLDER:
                grib_file = f"{MODEL}.{GRID}.{init_time}.{param}.{level}.{step}.grib"
                try:
                    if os.path.isfile(f"{path}/{grib_file}"):
                        print(f"{param}-{level}-{step} already exists.")
                        continue
                    else:
                        url = f"http://{CIPS_HOST}/cal/moddb_access.php?user={CIPS_USER}&mode=web&dateRun={init_time}&model={MODEL}&grid={GRID}&subGrid=&range={step}&level={level}&paramAlias={param}&format=grib&output=binary"
                        print(f"Downloading {param} {level} {step} ...")
                        response = requests.get(url, stream=True, allow_redirects=True)
                        if response.status_code == 200 and len(response.content) > 0:
                            with open(f"{path}/{grib_file}", 'wb') as f:
                                f.write(response.content)
                            print(f"{param}-{level}-{step} downloaded.")
                        else:
                            print(f"Failed to download {param}-{level}-{step}.")
                            failed_file.append(grib_file)
                            failed_url.append(url)
                            continue
                except (Timeout, RequestException, HTTPError, ConnectionError) as e:
                    print(f"Failed to download {param}-{level}-{step}. Error: {e}")
                    failed_file.append(grib_file)
                    failed_url.append(url)
                    continue

    failed_filename = f"{ROOT_DIR}/{TASK_DIR}/{MODEL}.{init_time}.FAILEDDATA.pkl"
    failed_data = {"failed_file": failed_file, "failed_url": failed_url}
    if len(failed_file) > 0:
        with open(failed_filename, "wb") as f:
            pickle.dump(failed_data, f)
    else:
        print("All files downloaded successfully.")
        failed_data = None
        with open(failed_filename, "wb") as f:
            pickle.dump(failed_data, f)

    return failed_filename


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NWP Downloader")
    parser.add_argument("-c", "--config", type=str, help="Path to Configuration file")
    parser.add_argument("-i","--init",type=int, help="Initial time (latest or YYYYMMDDHHMMSS)")
    args = parser.parse_args()

    if args.init == 'latest':
        print("Checking initialization time ...")
        if 7 < datetime.utcnow().hour <= 18:
            init_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        elif datetime.utcnow().hour in [0, 1, 2, 3, 4, 5, 6, 7]:
            init_time = (datetime.utcnow() - timedelta(days=1)).replace(hour=12, minute=0, second=0)
        else:
            init_time = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
        init_time = init_time.strftime('%Y%m%d%H%M%S')
    else:
        init_time = args.init
    print(f"Initialization time: {init_time}")

    if args.config:
        CONFIG_DIR = args.config
        if not os.path.exists(CONFIG_DIR):
            print("Configuration file does not exist.")
            exit()
        else:
            failed_filename = main(CONFIG_DIR)


