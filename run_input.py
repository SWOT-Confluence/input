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
import os
from pathlib import Path
import sys

# Third-party imports
import boto3
import botocore
import glob
from random import randint
from time import sleep

# Local imports
from input.Input import Input
from input.extract.ExtractLake import ExtractLake
from input.extract.ExtractRiver import ExtractRiver
from input.extract.exceptions import ReachNodeMismatch
from input.write.WriteLake import WriteLake
from input.write.WriteRiver import WriteRiver

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Retrieve a list of S3 URIs")
    arg_parser.add_argument("-i",
                            "--index",
                            type=int,
                            help="Index to specify input data to execute on, value of -235 indicates AWS selection")
    arg_parser.add_argument("-r",
                            "--rnjson",
                            type=str,
                            help="Path to the reach node json file or lakes json file",
                            default="reach_node.json")
    arg_parser.add_argument("-p",
                            "--cpjson",
                            type=str,
                            help="Path to the cycle pass json file",
                            default="cycle_pass.json")
    arg_parser.add_argument("-s",
                            "--shpjson",
                            type=str,
                            help="Path to the shapefile list json file",
                            default="s3_list_local.json")
    arg_parser.add_argument("-e",
                            "--rshpjson",
                            type=str,
                            help="Path to the reach S3 list json file",
                            default="s3_reach.json")
    arg_parser.add_argument("-c",
                            "--context",
                            type=str,
                            choices=["river", "lake"],
                            help="Context to retrieve data for: 'river' or 'lake'",
                            default="river")
    arg_parser.add_argument("-d",
                            "--directory",
                            type=str,
                            help="Directory to output data to")
    arg_parser.add_argument("-l",
                            "--local",
                            action='store_true',
                            help="Indicates local run of program")
    arg_parser.add_argument("-f",
                            "--shapefiledir",
                            type=str,
                            help="Directory of local shapefiles")
    return arg_parser

# def get_creds():
#     """Return AWS S3 credentials to access S3 shapefiles."""
    
#     ssm_client = boto3.client('ssm', region_name="us-west-2")
#     creds = {}
#     try:
#         creds["access_key"] = ssm_client.get_parameter(Name="s3_creds_key", WithDecryption=True)["Parameter"]["Value"]
#         creds["secret"] = ssm_client.get_parameter(Name="s3_creds_secret", WithDecryption=True)["Parameter"]["Value"]
#         creds["token"] = ssm_client.get_parameter(Name="s3_creds_token", WithDecryption=True)["Parameter"]["Value"]
#     except botocore.exceptions.ClientError as e:
#         raise e
#     else:
#         return creds

def get_creds():
    """Return AWS S3 credentials to access S3 shapefiles."""
    
    ssm_client = boto3.client('ssm', region_name="us-west-2")
    creds = {}
    retry_count = 10
    while retry_count>0:
        try:
            creds["access_key"] = ssm_client.get_parameter(Name="s3_creds_key", WithDecryption=True)["Parameter"]["Value"]
            creds["secret"] = ssm_client.get_parameter(Name="s3_creds_secret", WithDecryption=True)["Parameter"]["Value"]
            creds["token"] = ssm_client.get_parameter(Name="s3_creds_token", WithDecryption=True)["Parameter"]["Value"]
            retry_count = -999
        except:
            print('Error pulling credentials, retrying:', retry_count)
            retry_count-=1
            sleep(randint(1,300))
    if retry_count == 0:
        try:
            print('Final Try...')
            creds["access_key"] = ssm_client.get_parameter(Name="s3_creds_key", WithDecryption=True)["Parameter"]["Value"]
            creds["secret"] = ssm_client.get_parameter(Name="s3_creds_secret", WithDecryption=True)["Parameter"]["Value"]
            creds["token"] = ssm_client.get_parameter(Name="s3_creds_token", WithDecryption=True)["Parameter"]["Value"]
            retry_count = -999
        except botocore.exceptions.ClientError as e:
            raise e

    else:
        return creds
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
        
        i = int(index) if index != -235 else int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
        with open(json_file) as json_file:
            data = json.load(json_file)[i]
        return data

def select_strategies(context, exe_data, shapefiles, cycle_pass, output_dir, creds=None):
    """Define and set strategies to execute Input operations.
    
    Program exits if context is not set.
    
    Parameters
    ----------
    context: str
        string indicator of data type
    shapefiles: list
        list of SWOT shapefiles
    reach-data: list
        list of data to indicate what to execute on
    cycle_pass: dict
        dict of cycle pass json data
    output_dir: Path
        Path to output directory
    creds: dict
        dict of AWS S3 credentials
        
    Returns
    -------
    Input object with appropriate strategies selected
    """
    
    if context == "river":
        er = ExtractRiver(exe_data[0], shapefiles, cycle_pass, output_dir, creds, exe_data[1])
        ew = WriteRiver(exe_data[0], output_dir, exe_data[1])
        input = Input(er, ew)
    elif context == "lake": 
        el = ExtractLake(exe_data, shapefiles, cycle_pass, output_dir, creds)
        wl = WriteLake(exe_data, output_dir)
        input = Input(el, wl)
    else:
        print("Incorrect context selected to execute input operations.")
        sys.exit(1)
    return input

def main():
    """Main method to execute Input class methods."""
    
    start = datetime.now()

    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
        
    # Get input data to run on
    exe_data = get_exe_data(args.index, args.rnjson)
    
    # Get cycle pass data
    with open(args.cpjson) as jf:
        cycle_pass = json.load(jf)
    
    # Get shapefiles

    '''
    Using the shapefile dir argument you can specify a group of shapefiles you would like to run input for
    without the use of a local S3 list file.

    This is helpful if you would like to run on all the shapefiles present, without subsetting.

    The S3 json is needed to subset, or run in AWS.
    '''
    
    with open(args.rshpjson) as jf:
        shapefiles = json.load(jf)[exe_data[0]]
        
    # Select strategy to run based on context
    if not args.local:
        print("Obtaining S3 credentials.")
        try:
            creds = get_creds()
        except botocore.exceptions.ClientError as error:
            print("Error trying to retreive data from parameter store.")
            print(error)
            print("Exiting program...")
            sys.exit(1)
        input = select_strategies(args.context, exe_data, shapefiles, \
            cycle_pass, Path(args.directory), creds)
    else:
        input = select_strategies(args.context, exe_data, shapefiles, \
            cycle_pass, Path(args.directory))
    
    # Execute strategies to retrieve SWOT data and save as a NETCDF
    try:    
        print("Executing input strategies.")
        input.execute_strategies()
        print(f"File written for: {input.write_strategy.swot_id}.")
    except ReachNodeMismatch:
        print("The observation times for reaches did not match the observation " \
            + "times for nodes.\nThis indicates an error and you should " \
            + "compare the cycle/passes for reaches and nodes.\nExiting program...")
        sys.exit(1)
    
    end = datetime.now()
    print(f"Total execution time: {end - start}.")

if __name__ == "__main__":
    main()