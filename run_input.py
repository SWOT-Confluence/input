"""Script to run Input module.

The Input module logs into PO.DAAC AWS S3 infrastructure (TODO) and the 
Confluence S3 infrastructure. The Input module extracts SWOT observations and
formats them as one NetCDF per reach while also copying the most recent version
of the SoS based on the "run_type" commandline argument. The Input module can
also pull USGS gage data which is dictated by the "pull" command line argument.

Command line arguments:
run_type: values should be "constrained" or "unconstrained"
pull: values should be "pull" or "no_pull"
Default is to run unconstrained with no pull.
If you specify constrained you must specify whether to pull gage data.
"""

# Standard imports
from datetime import datetime
from pathlib import Path
import sys

# Local imports
from input.Extract import Extract
from input.Login import Login
from input.Write import Write   
from input.gage_pull.GageAppend import GageAppend
from input.gage_pull.GagePull import GagePull

OUTPUT = Path("/mnt/data")

def main():

    # Command line arguments
    try:
        run_type = sys.argv[1]
        pull = sys.argv[2]
    except IndexError:
        run_type = "unconstrained"
        pull = "no_pull"
    print(f"Running on '{run_type}' data product with gage pull status of '{pull}'.")

    # Login
    print("Logging into AWS infrastructure.")
    login = Login()
    login.login()

    # Extract SWOT data
    print("Extracting SWOT data.")
    ext = Extract()
    ext.extract_data(login.confluence_fs)
    
    # Write SWOT data and copy SoS data
    print("Writing SWOT data to NetCDF.")
    write = Write(ext.node_data, ext.reach_data, OUTPUT)
    write.write_data()
    print("Downloading SoS.")
    write.copy_sos_data(login.confluence_fs, run_type)
    
    # Append USGS gage data to the SoS
    if run_type == "constrained" and pull == "pull":
        print("Pulling USGS gage data and appending to SoS.")
        gage_pull = GagePull(OUTPUT / "usgs" / "USGStargetsV3.nc", '1980-1-1', datetime.today().strftime("%Y-%m-%d"))
        gage_pull.pull()
        gage_append = GageAppend(OUTPUT / "sos", gage_pull.usgs_dict)
        gage_append.read_sos()
        gage_append.map_data()
        gage_append.append_data()

    print("Input operations complete.")

if __name__ == "__main__":
    
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")