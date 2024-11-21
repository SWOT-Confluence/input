"""Script to run Input module.
"""

# Standard imports
import argparse
import boto3
import botocore
from datetime import datetime
import json
import logging
import pandas as pd
import requests
import numpy as np
import glob
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
BASE_URL= 'https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries?'
REACH_FIELDS = ['pass_id','cycle_id','d_x_area', 'd_x_area_u', 'dark_frac', 'ice_clim_f', 'ice_dyn_f', 'n_good_nod', 'obs_frac_n', 
    'partial_f', 'reach_id', 'reach_q', 'slope', 'slope2','slope2_r_u','slope_r_u','slope2_u', 'slope_u' , 'time', 'time_str', 'width', 
    'width_u', 'wse', 'wse_u','wse_r_u', 'xovr_cal_q', 'xtrk_dist', 'p_length', 'p_width', 'reach_q_b']
NODE_FIELDS = ['dark_frac', 'ice_clim_f', 'ice_dyn_f', 'n_good_pix', 'node_id',
            'node_q', 'node_q_b', 'p_width','reach_id','time', 'time_str', 'width', 
    'width_u', 'wse', 'wse_u', 'wse_r_u','xovr_cal_q', 'xtrk_dist']
FLOAT_FILL = -999999999999
INT_FILL = -999
SSM_CLIENT = boto3.session.Session().client("ssm")
RETRY_COUNT = 10

logging.getLogger().setLevel(logging.INFO)

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Retrieve a list of S3 URIs")
    arg_parser.add_argument("-i",
                            "--index",
                            type=int,
                            help="Index to specify input data to execute on, value of -235 indicates AWS selection")
    arg_parser.add_argument("-r",
                            "--reachesjson",
                            type=str,
                            help="Path to the reaches.json",
                            default="/mnt/data/reaches_of_interest.json")
    arg_parser.add_argument("-o",
                            "--outdir",
                            type=str,
                            help="Directory to output data to",
                            default="/mnt/data/swot/")
    arg_parser.add_argument("-s",
                            "--sworddir",
                            type=str,
                            help="Directory containing SWORD files",
                            default="/mnt/data/sword/")
    arg_parser.add_argument("-t",
                            "--time",
                            type=str,
                            help="Time parameter to search",
                            default="&start_time=2020-09-01T00:00:00Z&end_time=2025-10-30T00:00:00Z&")
    arg_parser.add_argument("-v",
                            "--swordversion",
                            type=str,
                            help="Version of sword we are using",
                            default="16")
    arg_parser.add_argument("-p",
                            "--prefix",
                            type=str,
                            help="Prefix for AWS environment.",
                            default="")
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
    
    out_df = df_filtered.iloc[(df_filtered['datetime'] - row['datetime']).abs().argsort()[:1]].iloc[0]

    return out_df


def get_reach_nodes(rootgrp, reach_id):
    """Get node ids from SWORD."""

    all_nodes = []
    node_ids_indexes = np.where(rootgrp.groups['nodes'].variables['reach_id'][:].data.astype('U') == str(reach_id))
    if len(node_ids_indexes[0])!=0:
        for y in node_ids_indexes[0]:
            node_id = str(rootgrp.groups['nodes'].variables['node_id'][y].data.astype('U'))
            all_nodes.append(node_id)
    return list(set(all_nodes))


def pull_via_hydrocron(reach_or_node, id_of_interest, fields, date_range, api_key):
    """Preform Hydrocron API request."""

    fieldstrs = ','.join(fields)
    params = {
        "feature": reach_or_node,
        "feature_id": id_of_interest,
        "output": "csv",
        "start_time": date_range.split("&")[1].split("=")[1],
        "end_time": date_range.split("&")[2].split("=")[1],
        "fields": fieldstrs
    }
    headers = {}
    if api_key:
        headers["x-hydrocron-key"] = api_key
    logging.info("Query parameters: %s", params)

    retry_cnt = 0
    while retry_cnt < RETRY_COUNT:
        # pull data from HydroCron into res variable
        try:
            data = requests.get(url=BASE_URL, headers=headers, params=params).json()
            logging.info('Hydrocron query: %s', data)
        except Exception as e:
            logging.info('Error pulling from hydrocron:%s', e)    # retry
            retry_cnt += 1
            time.sleep(random.uniform(1, 30))
            continue

        # check if limit has been exceeded
        if 'message' in data.keys():
            if data['message'] == 'Limit Exceeded':
                logging.info("Hydrocron request limit reached for API key. Exiting...")
                sys.exit(0)

        # check that it worked
        if 'error' in data.keys():    # retry
            retry_cnt += 1
            logging.info('Error pulling data: %s', data['error'])
            time.sleep(random.uniform(1, 30))
        elif 'status' in data.keys():
            if data['status']=='200 OK':
                # loads data into df
                df = data['results']['csv']
                df = pd.read_csv(StringIO(df))
                retry_cnt = 999

            else:
                retry_cnt += 1
                logging.info('Something went wrong: retrying')
                time.sleep(random.uniform(1, 30))
        else:
            retry_cnt += 1
            logging.info('Something went wrong: data not pulled or not stashed in dictionary correctly')
            time.sleep(random.uniform(1, 30))

    if retry_cnt != 999:
        logging.info('Failed to pull %s - %s', reach_or_node, id_of_interest)
        if reach_or_node == "Reach":
            logging.info("Failed to pull reach... exiting...")
            sys.exit(0)

    return df


def process_reach_via_hydrocron(reachid, nodeids, date_range, prefix):
    """Retrieve reach and node data from Hydrocron."""

    logging.info("Processing reach ID: %s", reachid)
    
    # retrieve API key
    try:
        api_key = SSM_CLIENT.get_parameter(Name=f"{prefix}-hydrocron-key", WithDecryption=True)["Parameter"]["Value"]
        logging.info("Querying with Hydrocron API key.")
    except botocore.exceptions.ClientError as error:
        api_key = ""
        logging.error(error)
        logging.info("Not querying with Hydrocron API key.")

    reach_df = pull_via_hydrocron('Reach', reachid, REACH_FIELDS, date_range, api_key)
    reach_df['datetime'] = reach_df['time_str'].apply(
        lambda x: pd.to_datetime(x) if x != "no_data" else pd.NaT
    )
    reach_df['cycle_pass'] = reach_df['cycle_id'].astype(str) + '_' + reach_df['pass_id'].astype(str)

    area_fit_dict = False
    if np.all((reach_df["d_x_area"] == FLOAT_FILL)):
        logging.info('Calculating HWS...')
        IO=HWS_IO(swot_dataset = reach_df, nt = len(reach_df))
        D=DomainHWS(IO.ObsData)
        hws_obj = CalculateHWS(D, IO.ObsData)
        if len(hws_obj.dAall) == 1:
            hws_obj.dAall = hws_obj.dAall[0]
        reach_df["d_x_area"] = hws_obj.dAall
        try:
            area_fit_dict = hws_obj.area_fit
        except:
            area_fit_dict = False

    node_df_list = []
    for nodeid in nodeids:

        logging.info("Processing node ID: %s", nodeid)
        node_df = pull_via_hydrocron('Node', nodeid, NODE_FIELDS, date_range, api_key)

        # Convert datetime strings to datetime objects
        node_df['datetime'] = node_df['time_str'].apply(
            lambda x: pd.to_datetime(x) if x != "no_data" else pd.NaT
        )

        # Extract dates
        reach_df['date'] = reach_df['datetime'].dt.date
        node_df['date'] = node_df['datetime'].dt.date

        # Find the closest datetimes for each date in reach_df
        closest_data = reach_df.apply(find_closest_date, df=node_df, axis=1)

        # Filtering columns: Keep only columns whose names are not integers (left over from the concat)
        closest_data = closest_data.loc[:, ~closest_data.columns.to_series().apply(lambda x: isinstance(x, int))]
        if len(list(closest_data.columns)) == 0:
            closest_data = pd.DataFrame(columns = list(node_df.columns))

        # Combine the original time_str with the closest data from node_df
        extra_fields = ['d_x_area', 'd_x_area_u', 'slope', 'slope2','slope2_r_u','slope_r_u','slope2_u', 'slope_u', 'cycle_pass']
        
        try:
            final_df = pd.concat([reach_df[['time_str']], closest_data.reset_index(drop=False)[NODE_FIELDS]], axis=1)
        except:
            raise
        final_df[extra_fields] = reach_df[extra_fields]

        # node_q wrong datatype
        cols_to_convert = ['node_q', 'ice_clim_f', 'ice_dyn_f', 'node_q_b', 'n_good_pix', 'xovr_cal_q']
        final_df[cols_to_convert] = final_df[cols_to_convert].apply(pd.to_numeric, downcast='integer').fillna(INT_FILL)

        node_df_list.append(final_df)

    return reach_df, node_df_list, area_fit_dict


def prep_output(reach_df, node_df_list):
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


def get_reachids(reachjson, index_to_run):
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

    return data[index]


def load_sword(reachid, sworddir, sword_version):
    cont_map = {
        '1':'af',
        '4':'as',
        '3':'as',
        '2':'eu',
        '7':'na',
        '8':'na',
        '9':'na',
        '5':'oc',
        '6':'sa'
    }

    sword_path = os.path.join(sworddir, cont_map[str(reachid)[0]] + f'_sword_v{sword_version}.nc')
    sword = netCDF4.Dataset(sword_path)

    return sword


def main():
    """Main method to execute Input class methods."""
    start = datetime.now()

    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()

    index_to_run = args.index
    reachjson = args.reachesjson
    outdir = args.outdir
    sworddir = args.sworddir
    date_range = args.time
    swordversion = args.swordversion
    prefix = args.prefix

    # pull sword and find all reach data
    reachid = get_reachids(reachjson,index_to_run)['reach_id']

    # map reach id to sword and load sword
    sword = load_sword(reachid, sworddir, swordversion)

    # find node ids for reach, also close sos
    nodeids = get_reach_nodes(sword, reachid)
    sword.close()

    # Pull observation data using hydrocron
    reach_df, node_df_list, area_fit_dict = process_reach_via_hydrocron(reachid, nodeids, date_range, prefix)

    # parse hydrocron returns
    output_data = prep_output(reach_df, node_df_list)

    # write out parsed data to timeseries file
    HCWrite.write_data(swot_id=reachid, node_ids=nodeids, data = output_data,area_fit_dict = area_fit_dict, output_dir = outdir)
    
    end = datetime.now()
    logging.info("Total execution time: %s", end - start)


if __name__ == "__main__":
    main()