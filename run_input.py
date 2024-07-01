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
import glob
import netCDF4
import os
import numpy as np
from io import StringIO

# Local imports
import input.write.HydrocronWrite as HCWrite


# global variables
REACH_FIELDS = ['pass_id','cycle_id','d_x_area', 'd_x_area_u', 'dark_frac', 'ice_clim_f', 'ice_dyn_f', 'n_good_nod', 'obs_frac_n', 
    'partial_f', 'reach_id', 'reach_q', 'slope', 'slope2','slope2_r_u','slope_r_u','slope2_u', 'slope_u' , 'time', 'time_str', 'width', 
    'width_u', 'wse', 'wse_u','wse_r_u', 'xovr_cal_q', 'xtrk_dist', 'p_length', 'p_width', 'reach_q_b']


# NODE_FIELDS = ['d_x_area', 'd_x_area_u', 'dark_frac', 'ice_clim_f', 'ice_dyn_f', 'n_good_pix', 'node_id',
#             'node_q', 'node_q_b','reach_id','slope', 'slope2_u', 'slope_u', 'slope2', 'time', 'time_str', 'width', 
#       'width_u', 'wse', 'wse_u', 'xovr_cal_q']
NODE_FIELDS = ['dark_frac', 'ice_clim_f', 'ice_dyn_f', 'n_good_pix', 'node_id',
            'node_q', 'node_q_b', 'p_width','reach_id','time', 'time_str', 'width', 
    'width_u', 'wse', 'wse_u', 'wse_r_u','xovr_cal_q', 'xtrk_dist']

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




# Function to find the closest datetime
def find_closest_date(row, df):
    date = row['date']
    out_df = pd.Series([None] * len(df.columns))
    out_df.columns = df.columns

    if NODE_FIELDS[0] not in list(df.columns):
        print('here is df')
        print(df)
        raise

    if pd.isna(date):
        print('returning empty, date')
        return out_df

    df_filtered = df[df['date'] == date]

    if df_filtered.empty:
        return out_df
    
    
    out_df = df_filtered.iloc[(df_filtered['datetime'] - row['datetime']).abs().argsort()[:1]].iloc[0]
    # print('returnning sucess')
    print(out_df[NODE_FIELDS])
    # print(out_df['foo'])


    return out_df



def get_reach_nodes(rootgrp, reach_id):


    # add mapping

    all_nodes = []

    # files = glob.glob(os.path.join(data_dir, '*'))
    # print(f'Searching across {len(files)} continents for nodes...')

    # for i in files:

        # rootgrp = netCDF4.Dataset(i, "r", format="NETCDF4")

    node_ids_indexes = np.where(rootgrp.groups['nodes'].variables['reach_id'][:].data.astype('U') == str(reach_id))

    if len(node_ids_indexes[0])!=0:
        for y in node_ids_indexes[0]:
            node_id = str(rootgrp.groups['nodes'].variables['node_id'][y].data.astype('U'))
            all_nodes.append(node_id)



        # all_nodes.extend(node_ids[0].tolist())

    rootgrp.close()

    print(f'Found {len(set(all_nodes))} nodes...')
    return list(set(all_nodes))


def pull_via_hydrocron(reach_or_node, id_of_interest, fields, time):
    baseurl= 'https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries?'
    
    fieldstrs=''

    for field in fields:
        if fieldstrs:
            field = ','+field
        fieldstrs+=field
        
    dataformat='csv' #switch this to csv to avoid getting all the data

    url=baseurl + f'feature={reach_or_node}&feature_id=' +  str(id_of_interest) + time + 'output=' + dataformat + '&fields=' + fieldstrs
    # df = pd.Dataframe(columns = fields)
    retry_cnt = 0
    while retry_cnt < 10:
        # pull data from HydroChron into res variable
        data = requests.get(url).json()
        # print(res)

        # load data into a dictionary
        # data=json.loads(res.text)


        # check that it worked
        if 'error' in data.keys():
            retry_cnt += 1
            print('Error pulling data:',data['error'])
        elif 'status' in data.keys():
            if data['status']=='200 OK':
                # loads data into df
                df = data['results']['csv']
                df = pd.read_csv(StringIO(df))
                retry_cnt = 999
                # print(df)
                print('Successfully pulled data and put in dictionary')
            else:
                retry_cnt += 1
                print(data)
                print('Something went wrong: retrying')
        else:
            retry_cnt += 1
            print(data)
            print('Something went wrong: data not pulled or not stashed in dictionary correctly')

    if retry_cnt != 999:
        # df = pd.Dataframe(columns = fields)
        raise ValueError('Failed to pull node_df')






    # OLD PARSING FOR GEOJSON
    # df=pd.DataFrame(columns=fields)

    # for feature in data['results']['geojson']['features']:    
    #     #rowdata=[feature['properties']['cycle_id'],feature['properties']['pass_id'],feature['properties']['time_str'],feature['properties']['wse'],feature['properties']['reach_q']]    
        
    #     data_els=feature['properties']
        
    #     rowdata=[]
    #     for field in fields:
    #         if field == 'slope':
    #             datafield=float(data_els[field])
    #         else:
    #             datafield=data_els[field]
                
    #         rowdata.append(datafield)
        
    #     df.loc[len(df.index)]=rowdata
    return df

def process_reach_via_hydrocron(reachid, nodeids, time):



    reach_df = pull_via_hydrocron('Reach', reachid, REACH_FIELDS, time)
    # reach_df['time_str_parse'] = reach_df['time_str'].str[:10]
    reach_df['datetime'] = pd.to_datetime(reach_df['time_str'], errors='coerce')
    reach_df['cycle_pass'] = reach_df['cycle_id'].astype(str) + '_' + reach_df['pass_id'].astype(str)
    # right_df['time_dt'] = pd.to_datetime(right_df['time_str'])
    # print('reach parse')
    # print(reach_df['time_str_parse'])
    # print(reach_df.shape, 'reach_shape')




    node_df_list = []
    for nodeid in nodeids:
        node_df = pull_via_hydrocron('Node', nodeid, NODE_FIELDS, time)

        # filter by reach observed days and average duplicate indexes
        # node_df['time_str_parse'] = node_df['time_str'].str[:10]
        # node_df = node_df.reset_index()
        # node_df = node_df.rename(columns=lambda x: x + '_right' if x != 'time_str_parse' else x)
        # merged = pd.merge(reach_df, node_df,on='time_str_parse', how='left')
        # result_df = merged[['time_str_parse'] + [col for col in node_df.columns if col != 'time_str_parse']]
        # result_df = result_df.rename(columns=lambda x: x.replace('_right', '') if x != 'time_str_parse' else x)
        # result_df = result_df.groupby('time_str_parse').mean()

        # Convert datetime strings to datetime objects
        # reach_df['datetime'] = pd.to_datetime(reach_df['time_str'], errors='coerce')
        node_df['datetime'] = pd.to_datetime(node_df['time_str'], errors='coerce')

        # Extract dates
        reach_df['date'] = reach_df['datetime'].dt.date
        node_df['date'] = node_df['datetime'].dt.date



        # Find the closest datetimes for each date in reach_df
        closest_data = reach_df.apply(find_closest_date, df=node_df, axis=1)
        # raise

        # Filtering columns: Keep only columns whose names are not integers (left over from the concat)
        closest_data = closest_data.loc[:, ~closest_data.columns.to_series().apply(lambda x: isinstance(x, int))]
        if len(list(closest_data.columns)) == 0:
            print('it was zero')
            print(closest_data)
            closest_data = pd.DataFrame(columns = list(node_df.columns))
            # raise

        # Combine the original time_str with the closest data from node_df
        extra_fields = ['d_x_area', 'd_x_area_u', 'slope', 'slope2','slope2_r_u','slope_r_u','slope2_u', 'slope_u', 'cycle_pass']
        # print(reach_df[NODE_FIELDS])
        # print(pd.concat([reach_df[['time_str']], closest_data.reset_index(drop=True)[NODE_FIELDS]], axis=1))
        
        try:
            # final_df = pd.concat([reach_df[['time_str']], closest_data.reset_index(drop=True)[NODE_FIELDS]], axis=1)
            print('pre')
            print(closest_data)
            print(closest_data.reset_index(drop=False))
            print('no pree')
            final_df = pd.concat([reach_df[['time_str']], closest_data.reset_index(drop=False)[NODE_FIELDS]], axis=1)
        except:
            # print(closest_data)
            print(reach_df)
            print(list(closest_data))
            print(closest_data.reset_index(drop=True))
            print('theres the error')
            raise
        final_df[extra_fields] = reach_df[extra_fields]

        if final_df.shape[0] != reach_df.shape[0]:
            print('miss shapes')
            print(reach_df.shape)
            print(final_df.shape)
            print(final_df['time_str'])
        # return 'foo'

        # node_q wrong datatype
        cols_to_convert = ['node_q', 'ice_clim_f', 'ice_dyn_f', 'node_q_b', 'n_good_pix', 'xovr_cal_q']
        final_df[cols_to_convert] = final_df[cols_to_convert].apply(pd.to_numeric, downcast='integer').fillna(-999)



        node_df_list.append(final_df)




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

    return output_data
            
def get_reachids(reachjson,index_to_run):
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
    time = args.time
    swordversion = args.swordversion

    # pull sword and find all reach data
    reachid = get_reachids(reachjson,index_to_run)

    # map reach id to sword and load sword
    sword = load_sword(reachid, sworddir, swordversion)

    # find node ids for reach, also close sos
    nodeids = get_reach_nodes(sword, reachid)

    # Pull observation data using hydrocron
    reach_df, node_df_list = process_reach_via_hydrocron(reachid, nodeids, time)

    # parse hydrocron returns
    output_data = prep_output(reach_df, node_df_list)

    # write out parsed data to timeseries file
    HCWrite.write_data(swot_id=reachid, node_ids=nodeids, data = output_data, output_dir = outdir)
    
    end = datetime.now()
    print(f"Total execution time: {end - start}.")


if __name__ == "__main__":
    main()