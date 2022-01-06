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
from input.Extract import Extract
from input.Login import Login
from input.Write import Write

DATA = Path("/mnt/data")

def main():
    
    # Store command line arguments
    try:
        reach_node_json = sys.argv[1]
    except IndexError:
        reach_node_json = "reach_node.json"

    # Get continent to run on
    index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    with open(DATA / reach_node_json) as json_file:
        reach_data = json.load(json_file)[index]
    
    # Login
    print("Logging into AWS infrastructure.")
    login = Login()
    login.login()

    # Extract SWOT data
    print("Extracting SWOT data.")
    ext = Extract(login.confluence_fs, reach_data[0], reach_data[1])
    ext.extract_data()
    
    # Write SWOT data
    print("Writing SWOT data to NetCDF.")
    write = Write(ext.node_data, ext.reach_data, ext.obs_times, DATA)
    write.write_data(reach_data[0], reach_data[1])
    
    print("Input operations complete.")

if __name__ == "__main__":
    
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")