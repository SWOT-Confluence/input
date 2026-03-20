"""Script to run Input module."""

# Standard imports
import argparse
import boto3
from datetime import datetime
import json
import logging
import pandas as pd
import requests
import numpy as np
import netCDF4
import os
import numpy as np
from io import StringIO
import sys
import time
import random

# Local imports
import input.write.HydrocronWrite as HCWrite
from input.extract.CalculateHWS import CalculateHWS
from input.extract.DomainHWS import DomainHWS
from input.extract.HWS_IO import HWS_IO


# global variables
BASE_URL = "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries?"

# fmt: off
REACH_FIELDS = [
    "pass_id", "cycle_id", "d_x_area", "d_x_area_u", "dark_frac", "ice_clim_f", 
    "ice_dyn_f", "n_good_nod", "obs_frac_n", "partial_f", "reach_id", "reach_q", 
    "slope", "slope2", "slope2_r_u", "slope_r_u", "slope2_u", "slope_u", "time", 
    "time_str", "width", "width_u", "wse", "wse_u", "wse_r_u", "xovr_cal_q", 
    "xtrk_dist", "p_length", "p_width", "reach_q_b"
]
NODE_FIELDS = [
    "dark_frac", "ice_clim_f", "ice_dyn_f", "n_good_pix", "node_id", "node_q", 
    "node_q_b", "p_width", "reach_id", "time", "time_str", "width", "width_u", 
    "wse", "wse_u", "wse_r_u", "xovr_cal_q", "xtrk_dist"
]
EXTRA_FIELDS = [
    "d_x_area", "d_x_area_u", "slope", "slope2", "slope2_r_u", "slope_r_u", 
    "slope2_u", "slope_u", "cycle_pass"
]
COLS_TO_CONVERT = ["node_q", "ice_clim_f", "ice_dyn_f", "node_q_b", "n_good_pix", "xovr_cal_q"]
# fmt: on

FLOAT_FILL = -999999999999
INT_FILL = -999

CONT_MAP = {
    "1": "af",
    "4": "as",
    "3": "as",
    "2": "eu",
    "7": "na",
    "8": "na",
    "9": "na",
    "5": "oc",
    "6": "sa",
}

RETRY_COUNT = 10  # number of retries after failure
RANDOM_SLEEP = 30  # seconds

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S',
                    level=logging.INFO)

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Retrieve a list of S3 URIs")
    arg_parser.add_argument(
        "-i",
        "--index",
        type=int,
        help="Index to specify input data to execute on, value of -235 indicates AWS selection",
    )
    arg_parser.add_argument(
        "-a",
        "--range",
        type=int,
        help="Number of reaches to step through starting from the index.",
        default=1,
    )
    arg_parser.add_argument(
        "-k",
        "--skip",
        action="store_true",
        help="Skip processing if the NetCDF file for a reach already exists.",
        default=False,
    )
    arg_parser.add_argument(
        "-r",
        "--reachesjson",
        type=str,
        help="Path to the reaches.json",
        default="/mnt/data/reaches_of_interest.json",
    )
    arg_parser.add_argument(
        "-o",
        "--outdir",
        type=str,
        help="Directory to output data to",
        default="/mnt/data/swot/",
    )
    arg_parser.add_argument(
        "-s",
        "--sworddir",
        type=str,
        help="Directory containing SWORD files",
        default="/mnt/data/sword/",
    )
    arg_parser.add_argument(
        "-t",
        "--time",
        type=str,
        help="Time parameter to search",
        default="&start_time=2020-09-01T00:00:00Z&end_time=2026-12-30T00:00:00Z&",
    )
    arg_parser.add_argument(
        "-v",
        "--swordversion",
        type=str,
        help="Version of sword we are using",
        default="17b",
    )
    arg_parser.add_argument(
        "-p", "--prefix", type=str, help="Prefix for AWS environment.", default=""
    )
    arg_parser.add_argument(
        "-c",
        "--collection",
        type=str,
        help="Collection/product to use",
        default="SWOT_L2_HR_RiverSP_D",
    )

    return arg_parser


def get_exe_data(index, json_file):
    """Retrun dictionary of data required to execution input operations.
    
    Parameters
    ----------
    index: int
        integer to index JSON data on
    json_file: Path
        path to JSON file to pull data from
        
    Returns
    -------
    dictionary of execution data
    """
    
    with open(json_file) as json_file:
        data = json.load(json_file)[index]
    return data


# TODO: could be much faster with pd.merge_asof using dates as keys
def find_closest_date(row, df):
    """Function to find the closest datetime."""

    date = row['date']
    out_df = pd.Series([None] * len(df.columns))
    out_df.columns = df.columns

    if pd.isna(date):
        return out_df

    df_filtered = df[df['date'] == date]

    if df_filtered.empty:
        return out_df

    out_df = df_filtered.iloc[
        (df_filtered["datetime"] - row["datetime"]).abs().argsort()[:1]
    ].iloc[0]

    return out_df


def get_reach_nodes(rootgrp, reach_id):
    """Get node ids from SWORD."""

    all_nodes = []
    node_ids_indexes = np.where(
        rootgrp.groups["nodes"].variables["reach_id"][:].data.astype("U")
        == str(reach_id)
    )
    if len(node_ids_indexes[0]) != 0:
        for y in node_ids_indexes[0]:
            node_id = str(
                rootgrp.groups["nodes"].variables["node_id"][y].data.astype("U")
            )
            all_nodes.append(node_id)
    nodeids = list(set(all_nodes))
    nodeids.sort()
    return nodeids


def get_api_key(prefix):
    creds_file = os.path.expanduser("~/.hydrocron/credentials")
    if os.path.exists(creds_file):
        try:
            with open(creds_file) as f:
                creds = json.load(f)
            key = creds["api_key"] # Try to access
            logging.info("Using API key from credentials file: %s", creds_file)
            return key
        except (KeyError, json.JSONDecodeError) as e:
            # File exists but something is really wrong.
            logging.error("Failed to parse credentials file %s: %s", creds_file, e)
            return ""

    try:
        ssm = boto3.client("ssm")
        return ssm.get_parameter(Name=f"{prefix}-hydrocron-key", WithDecryption=True)[
            "Parameter"
        ]["Value"]
    except Exception as e:
        logging.error(f"SSM Error: {e}")
        return ""


def pull_via_hydrocron(
    reach_or_node, id_of_interest, fields, date_range, collection_name, api_key
):
    """Preform Hydrocron API request."""

    fieldstrs = ','.join(fields)
    params = {
        "feature": reach_or_node,
        "feature_id": id_of_interest,
        "output": "csv",
        "start_time": date_range.split("&")[1].split("=")[1],
        "end_time": date_range.split("&")[2].split("=")[1],
        "fields": fieldstrs,
        "collection_name": collection_name,
    }
    headers = {}
    if api_key:
        headers["x-hydrocron-key"] = api_key
    # logging.info("Query parameters: %s", params)

    df = None # default
    for _ in range(RETRY_COUNT):
        try:
            response = requests.get(url=BASE_URL, headers=headers, params=params)
            # logging.info(f"Hydrocron query: {response.url}")
            response.raise_for_status()  # raises HTTPError for 4xx/5xx
            data = response.json()

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code

            # Attempt to parse the error message from the response body
            try:
                error_payload = e.response.json()
                error_msg = error_payload.get("error", "")
            except (ValueError, AttributeError):
                error_msg = ""

            if status == 429:
                logging.error("Rate limit hit (HTTP 429). Exiting...")
                sys.exit(1)
            elif 400 <= status < 500:
                if "not found" in error_msg.lower():
                    # "error" : "400: Results with the specified Feature ID 11545300011 were not found"
                    logging.warning(f"Feature ID {id_of_interest} has no data in {collection_name}. Skipping...")
                    return # returns to reach_id loop to check next reach_id for data.
                
                logging.error(f"Client error, will not retry. Exiting... \n{status = }\n{error_payload = }")
                return
            else:
                logging.warning(f"Server error {status}, retrying...")
                time.sleep(random.uniform(1, RANDOM_SLEEP))
                continue

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.JSONDecodeError) as e:
            logging.warning(f"Request failed: {e}. Retrying...")
            time.sleep(random.uniform(1, RANDOM_SLEEP))
            continue

        except Exception as e:
            logging.warning(f"Unexpected exception: {e}. Retrying...")
            time.sleep(random.uniform(1, RANDOM_SLEEP))
            continue

        # HTTP was 200, now validate the JSON payload
        if 'message' in data:
            logging.error(f"Hydrocron returned unexpected message: {data['message']}. Exiting...")
            return

        elif 'error' in data:
            logging.warning(f"Hydrocron error in payload: {data['error']}. Retrying...")
            time.sleep(random.uniform(1, RANDOM_SLEEP))

        elif data.get('status') == '200 OK':
            try:
                df = pd.read_csv(StringIO(data['results']['csv']))
                # Success! 
                break
            except Exception as e:
                logging.warning(f"Exception while parsing the json data: {e}")
                time.sleep(random.uniform(1, RANDOM_SLEEP))

        else:
            logging.warning(f"Unrecognized response structure: {data}. Retrying...")
            time.sleep(random.uniform(1, RANDOM_SLEEP))

    return df


def process_reach_via_hydrocron(reachid, nodeids, date_range, collection_name, api_key):
    """Retrieve reach and node data from Hydrocron."""

    logging.info("Processing reach ID: %s", reachid)

    # pull reach data
    reach_df = pull_via_hydrocron(
        "Reach", reachid, REACH_FIELDS, date_range, collection_name, api_key
    )
    if reach_df is None:
        # If we have no reach data, we just return
        return 
    
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    reach_df["datetime"] = pd.to_datetime(reach_df["time_str"], format=fmt, errors="coerce")
    reach_df["cycle_pass"] = (
        reach_df["cycle_id"].astype(str) + "_" + reach_df["pass_id"].astype(str)
    )
    reach_df["date"] = reach_df["datetime"].dt.date

    # HWS calcs for reaches
    if np.all((reach_df["d_x_area"] == FLOAT_FILL)):
        logging.info('Calculating HWS...')
        logging.info('potato...')
        IO=HWS_IO(swot_dataset = reach_df, nt = len(reach_df))
        D=DomainHWS(IO.ObsData)
        hws_obj = CalculateHWS(D, IO.ObsData)
        if len(hws_obj.dAall) == 1:
            hws_obj.dAall = hws_obj.dAall[0]
        reach_df["d_x_area"] = hws_obj.dAall
        area_fit_dict = getattr(hws_obj, "area_fit", False)

    # Pull node data
    node_df_list = []
    for nodeid in nodeids:

        logging.info("Processing node ID: %s", nodeid)
        node_df = pull_via_hydrocron(
            "Node", nodeid, NODE_FIELDS, date_range, collection_name, api_key
        )
        if node_df is None:
            # If we have missing node data, continue trying the others.
            continue

        # Convert datetime strings to datetime objects
        node_df["datetime"] = pd.to_datetime(node_df["time_str"], format=fmt, errors="coerce")

        # Extract dates
        node_df["date"] = node_df["datetime"].dt.date

        # Find the closest datetimes for each date in reach_df
        closest_data = reach_df.apply(find_closest_date, df=node_df, axis=1)

        # Filtering columns: Keep only columns whose names are not integers (left over from the concat)
        closest_data = closest_data.loc[:, ~closest_data.columns.to_series().apply(lambda x: isinstance(x, int))]
        if len(list(closest_data.columns)) == 0:
            closest_data = pd.DataFrame(columns = list(node_df.columns))

        # Combine the original time_str with the closest data from node_df
        final_df = pd.concat(
            [reach_df[["time_str"]], closest_data.reset_index(drop=False)[NODE_FIELDS]],
            axis=1,
        )
        final_df[EXTRA_FIELDS] = reach_df[EXTRA_FIELDS]

        # node_q wrong datatype
        final_df[COLS_TO_CONVERT] = (
            final_df[COLS_TO_CONVERT]
            .apply(pd.to_numeric, downcast="integer")
            .fillna(INT_FILL)
        )

        node_df_list.append(final_df)

    return reach_df, node_df_list, area_fit_dict


def prep_output(reach_df, node_df_list):
    """Prep data for output to NetCDF."""

    output_data = {'reach':{}, 'node':{}}
    for header in reach_df.columns:
        output_data['reach'][header] = reach_df[header].values

    stacked_array = np.stack([df.values for df in node_df_list], axis=-1)

    # Transpose the array to get the desired shape (len(df) x num_dfs)
    final_arrays = [stacked_array[:, i, :].T for i in range(stacked_array.shape[1])]

    cnt = 0
    for header in node_df_list[0].columns:
        output_data['node'][header] = final_arrays[cnt]
        cnt += 1 

    return output_data


def get_reaches_by_continent(reach_ids):
    """Groups reach IDs by continent to minimize file I/O."""
    grouped = {}
    for rid in reach_ids:
        cont = CONT_MAP[str(rid)[0]]
        grouped.setdefault(cont, []).append(rid)
    return grouped


def get_reachids(reachjson: str, index_to_run: int, index_range: int) -> list[int]:
    """Extract and return a list of reach identifiers from json file.
    
    Parameters
    ----------
    reachjson : str
        Path to the file that contains the list of reaches to process
    
        
    Returns
    -------
    list
        List of reaches identifiers
    """

    if index_to_run == -235:
        index=int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    else:
        index=index_to_run

    with open(reachjson) as jsonfile:
        data = json.load(jsonfile)

    reach_dict = data[index : (index + index_range)]
    return [d["reach_id"] for d in reach_dict]


def main():
    """Main method to execute Input class methods."""
    start = datetime.now()

    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()

    index_to_run = args.index
    index_range = args.range
    skip = args.skip
    reachjson = args.reachesjson
    outdir = args.outdir
    sworddir = args.sworddir
    date_range = args.time
    swordversion = args.swordversion
    prefix = args.prefix
    collection_name = args.collection

    all_reach_ids = get_reachids(reachjson, index_to_run, index_range)
    if skip:
        all_reach_ids = [r for r in all_reach_ids if not HCWrite.check_file_exists(outdir, r)]
        if len(all_reach_ids) == 0:
            logging.info(
                "All reaches already downloaded to input dir. Remove the skip argument if you want to force redownload."
            )
            sys.exit(0)

    # not confident that batches will be grouped by continent.
    continent_groups = get_reaches_by_continent(all_reach_ids)

    api_key = get_api_key(prefix)

    for cont, cont_reach_ids in continent_groups.items():
        sword_path = os.path.join(sworddir, f"{cont}_sword_v{swordversion}.nc")
        with netCDF4.Dataset(sword_path) as sword:
            for reach_id in cont_reach_ids:
                nodeids = get_reach_nodes(sword, reach_id)

                reach_data = process_reach_via_hydrocron(reach_id, nodeids, date_range, collection_name, api_key)

                if reach_data is None:
                    logging.info(f"No data returned for {reach_id = }, moving on.")
                    continue
                else:
                    reach_df, node_df_list, area_fit_dict = reach_data

                logging.info("Located %s timesteps.", reach_df.shape[0])

                output_data = prep_output(reach_df, node_df_list)
                HCWrite.write_data(
                    swot_id=reach_id,
                    node_ids=nodeids,
                    data=output_data,
                    area_fit_dict=area_fit_dict,
                    output_dir=outdir,
                )

    end = datetime.now()
    logging.info("Total execution time: %s", end - start)


if __name__ == "__main__":
    main()
