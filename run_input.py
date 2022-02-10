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
from datetime import datetime
import json
import os
from pathlib import Path
import sys

# Local imports
from input.Input import Input
from input.extract.ExtractLake import ExtractLake
from input.extract.ExtractRiver import ExtractRiver
from input.Login import Login
from input.write.WriteLake import WriteLake
from input.write.WriteRiver import WriteRiver

# Constants
DATA = Path("/mnt/data")

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
        
        index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
        with open(DATA/ input_json) as json_file:
            reach_data = json.load(json_file)[index]
        return reach_data

def select_strategies(context, confluence_fs, exe_data, cycle_pass_json):
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
    cycle_pass_json: Path
        path to cycle pass JSON file
        
    Returns
    -------
    Input object with appropriate strategies selected
    """
    
    if context == "river":
        er = ExtractRiver(confluence_fs, exe_data, cycle_pass_json)
        ew = WriteRiver(exe_data[0], DATA, exe_data[1])
        input = Input(er, ew)
    elif context == "lake": 
        el = ExtractLake(confluence_fs, exe_data, cycle_pass_json)
        wl = WriteLake(exe_data, DATA)
        input = Input(el, wl)
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
        cycle_pass_json = sys.argv[2]
        context = sys.argv[3]
    except IndexError:
        input_json = "reach_node.json"
        cycle_pass_json = "cycle_passes.json"
        context = "river"
    exe_data = get_exe_data(input_json)

    # Log into S3
    login = Login()
    login.login()
    
    # Create Input and set execution strategy
    print(f"Extracting and writing {context} data for identifier: {exe_data[0]}.")
    input = select_strategies(context, login.confluence_fs, exe_data, cycle_pass_json)
    input.execute_strategies()
    
    end = datetime.now()
    print(f"Total execution time: {end - start}.")

if __name__ == "__main__":
    main()