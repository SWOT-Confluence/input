"""Script to run Input module.

The Input module logs into PO.DAAC AWS S3 infrastructure (TODO) and the 
Confluence S3 infrastructure. The Input module extracts SWOT observations and
formats them as one NetCDF per reach.
"""

# Standard imports
from datetime import datetime
import json
import os
from pathlib import Path
import sys

# Local imports
from input.Input import Input
from input.extract.ExtractRiver import ExtractRiver
from input.Login import Login

# Constants
DATA = Path("/mnt/data")
DATA = Path("/home/nikki/Documents/confluence/workspace/input/data")

def get_exe_data(input_json):
        """Retrun dictionary of data required to execution input operations.
        
        Parameters
        ----------
        input_json: str
            string name of json file used to detect what to execute on
            
        Returns
        -------
        dictionary of execution data
        """
        
        # index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
        index = 194
        with open(DATA/ input_json) as json_file:
            reach_data = json.load(json_file)[index]
        return reach_data

def select_strategies(context, confluence_fs, exe_data):
    """Define and set strategies to execute Input operations.
    
    Program exits if context is not set.
    
    Parameters
    ----------
    context: str
        string indicator of data type
    confluence_fs: S3FileSystem
        references Confluence S3 buckets
    exe_data: list
        list of data to indicate what to execute on
        
    Returns
    -------
    Input object with appropriate strategies selected
    """
    
    if context == "river":
        er = ExtractRiver(confluence_fs, exe_data[0], exe_data[1])
        # input = Input(ExtractRiver(), WriteRiver())
        input = Input(er, None)
    elif context == "lake": 
        # input = Input(ExtractLake(), WriteLake())
        input = Input(None, None)
    else:
        print("Incorrect context selected to execute input operations.")
        sys.exit(1)
    return input

def main():
    """Main method to execute Input class methods."""
    
    start = datetime.now()
    
    # Store command line arguments
    try:
        input_json = sys.argv[1]
        context = sys.argv[2]
    except IndexError:
        input_json = "reach_node.json"
        context = "river"
    exe_data = get_exe_data(input_json)
    
    # Log into S3
    login = Login()
    login.login()
    
    # Create Input and set execution strategy
    input = select_strategies(context, login.confluence_fs, exe_data)
    input.execute_strategies()
    
    end = datetime.now()
    print(f"Total execution time: {end - start}.")

if __name__ == "__main__":
    main()