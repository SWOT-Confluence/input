"""Script to run Input module.

The Input module logs into PO.DAAC AWS S3 infrastructure (TODO) and the 
Confluence S3 infrastructure. The Input module extracts SWOT observations and
formats them as one NetCDF per reach.

Command line arguments:
[1] JSON file name, e.g. -> "reach_node.json" or "lake.json"
[2] Context of run, e.g. -> "lake" or "river"
DEFAULT json file is "reach_node.json" and runs in "river" context.
"""

# Standard imports
import argparse
from datetime import datetime
import json
import pandas as pd
import requests
import numpy as np
# import os
# from pathlib import Path
# import sys

# # Third-party imports
# import boto3
# import botocore
# import glob
# from random import randint
# from time import sleep

# # Local imports
# from input.Input import Input
# from input.extract.ExtractLake import ExtractLake
# from input.extract.ExtractRiver import ExtractRiver
# from input.extract.exceptions import ReachNodeMismatch
# from input.write.WriteLake import WriteLake
import input.write.HydrocronWrite as HCWrite

# def create_args():
#     """Create and return argparser with arguments."""

    # arg_parser = argparse.ArgumentParser(description="Retrieve a list of S3 URIs")
    # arg_parser.add_argument("-i",
    #                         "--index",
    #                         type=int,
    #                         help="Index to specify input data to execute on, value of -235 indicates AWS selection")
    # arg_parser.add_argument("-r",
    #                         "--rnjson",
    #                         type=str,
    #                         help="Path to the reach node json file or lakes json file",
    #                         default="reach_node.json")
    # arg_parser.add_argument("-p",
    #                         "--cpjson",
    #                         type=str,
    #                         help="Path to the cycle pass json file",
    #                         default="cycle_pass.json")
    # arg_parser.add_argument("-s",
    #                         "--shpjson",
    #                         type=str,
    #                         help="Path to the shapefile list json file",
    #                         default="s3_list_local.json")
    # arg_parser.add_argument("-e",
    #                         "--rshpjson",
    #                         type=str,
    #                         help="Path to the reach S3 list json file",
    #                         default="s3_reach.json")
    # arg_parser.add_argument("-c",
    #                         "--context",
    #                         type=str,
    #                         choices=["river", "lake"],
    #                         help="Context to retrieve data for: 'river' or 'lake'",
    #                         default="river")
    # arg_parser.add_argument("-d",
    #                         "--directory",
    #                         type=str,
    #                         help="Directory to output data to")
    # arg_parser.add_argument("-l",
    #                         "--local",
    #                         action='store_true',
    #                         help="Indicates local run of program")
    # arg_parser.add_argument("-f",
    #                         "--shapefiledir",
    #                         type=str,
    #                         help="Directory of local shapefiles")
    # arg_parser.add_argument("-n",
    #                         "--chunk_number",
    #                         type=int,
    #                         help="Number indicating what chunk to run on ")
    # return arg_parser

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

def find_node_ids_given_reach_id(sword, reach_id):
    return ['12554000060011','12554000060011']

def pull_via_hydrocron(reach_or_node, id_of_interest, fields, time):
    baseurl= 'https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries?'
    
    fieldstrs=''

    for field in fields:
        if fieldstrs:
            field = ','+field
        fieldstrs+=field
        
    dataformat='geojson' #switch this to csv to avoid getting all the data

    url=baseurl + f'feature={reach_or_node}&feature_id=' +  id_of_interest + time + 'output=' + dataformat + '&fields=' + fieldstrs


    # pull data from HydroChron into res variable
    res = requests.get(url)

    # load data into a dictionary
    data=json.loads(res.text)

    # check that it worked
    if 'error' in data.keys():
        print('Error pulling data:',data['error'])
    elif data['status']=='200 OK':
        print('Successfully pulled data and put in dictionary')
    else:
        print('Something went wrong: data not pulled or not stashed in dictionary correctly')

    
    df=pd.DataFrame(columns=fields)

    for feature in data['results']['geojson']['features']:    
        #rowdata=[feature['properties']['cycle_id'],feature['properties']['pass_id'],feature['properties']['time_str'],feature['properties']['wse'],feature['properties']['reach_q']]    
        
        data_els=feature['properties']
        
        rowdata=[]
        for field in fields:
            if field == 'slope':
                datafield=float(data_els[field])
            else:
                datafield=data_els[field]
                
            rowdata.append(datafield)
        
        df.loc[len(df.index)]=rowdata
    return df

def process_reach_via_hydrocron(reachid, nodeids, time):

      reach_fields = ['pass_id','d_x_area', 'd_x_area_u', 'dark_frac', 'ice_clim_f', 'ice_dyn_f', 'n_good_nod', 'obs_frac_n', 
            'partial_f', 'reach_id', 'reach_q', 'slope', 'slope2_u', 'slope_u', 'slope2', 'time', 'time_str', 'width', 
            'width_u', 'wse', 'wse_u', 'xovr_cal_q']
      
      # node_fields = ['d_x_area', 'd_x_area_u', 'dark_frac', 'ice_clim_f', 'ice_dyn_f', 'n_good_pix', 'node_id',
      #             'node_q', 'node_q_b','reach_id','slope', 'slope2_u', 'slope_u', 'slope2', 'time', 'time_str', 'width', 
      #       'width_u', 'wse', 'wse_u', 'xovr_cal_q']
      node_fields = ['dark_frac', 'ice_clim_f', 'ice_dyn_f', 'n_good_pix', 'node_id',
                  'node_q', 'node_q_b','reach_id','time', 'time_str', 'width', 
            'width_u', 'wse', 'wse_u', 'xovr_cal_q']

      reach_df = pull_via_hydrocron('Reach', reachid, reach_fields, time)

      node_df_list = []
      for nodeid in nodeids:
            node_df = pull_via_hydrocron('Node', nodeid, node_fields, time)
            node_df_list.append(node_df)



      return reach_df, node_df_list

def prep_output(reach_df, node_df_list):
    output_data = {'reach':{}, 'node':{}}
    for header in reach_df.columns:
        output_data['reach'][header] = reach_df[header].values
    cnt = 0
    # for node_df in node_df_list:
    #     for header in list(node_df.columns()):
    
    # Stack the DataFrames along a new axis to create a 3D numpy array
    # print('here is one node', node_df_list[0])
    # print('here is node two', node_df_list[1])
    stacked_array = np.stack([df.values for df in node_df_list], axis=-1)

    # Transpose the array to get the desired shape (len(df) x num_dfs)
    final_arrays = [stacked_array[:, i, :].T for i in range(stacked_array.shape[1])]
    cnt = 0
    for header in node_df_list[0].columns:
        output_data['node'][header] = final_arrays[cnt]
        cnt += 1 
    # print(output_data['node']['time'])
    # print(output_data['node']['time_str'])
    return output_data
            



def main():
    """Main method to execute Input class methods."""
    
    start = datetime.now()

    # # Command line arguments
    # arg_parser = create_args()
    # args = arg_parser.parse_args()
    # index = args.index
        
    # # Get input data to run on
    # if args.chunk_number is not None:
    #     # run_jsons = glob.glob(args.rnjson.replace('.json', '*'))
    #     run_json = args.rnjson.replace('.json', f'_{args.chunk_number}.json')
    # else:
    #     run_json = args.rnjson

    # index = int(index) if index != -235 else int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))

    # exe_data = get_exe_data(index, args.rnjson)

    # reachid = exe_data[0]
    # print(f"Running on reach: {reachid} (index number {index}).")

    # setup pull
    sword = 'foo'

    # pull sword and find all reach data
    reachid='12554000061'

    nodeids = find_node_ids_given_reach_id(sword, reachid)
    nx = len(nodeids)

    time='&start_time=2020-09-01T00:00:00Z&end_time=2025-10-30T00:00:00Z&'

    reach_df, node_df_list = process_reach_via_hydrocron(reachid, nodeids, time)

    output_data = prep_output(reach_df, node_df_list)
# swot_id, node_ids, data, output_dir
    HCWrite.write_data(swot_id=reachid, node_ids=nodeids, data = output_data, output_dir = '.')

    

    # except ReachNodeMismatch:
    #     print("The observation times for reaches did not match the observation " \
    #         + "times for nodes.\nThis indicates an error and you should " \
    #         + "compare the cycle/passes for reaches and nodes.\nExiting program...")
    #     sys.exit(1)
    
    end = datetime.now()
    print(f"Total execution time: {end - start}.")




#     # setup pull
# sword = 'foo'

# # pull sword and find all reach data
# reachid='12554000061'

# nodeids = find_node_ids_given_reach_id(sword, reachid)
# nx = len(nodeids)

# time='&start_time=2020-09-01T00:00:00Z&end_time=2025-10-30T00:00:00Z&'

# reach_df, node_df_list = process_reach_via_hydrocron(reachid, nodeids, time)

# nx = len(nodeids)
# nt = len(reach_df)

if __name__ == "__main__":
    main()