"""Script to run Input module.

The Input module logs into PO.DAAC AWS S3 infrastructure (TODO) and the 
Confluence S3 infrastructure. The Input module extracts SWOT observations and
formats them as one NetCDF per reach while also copying the most recent version
of the SoS based on the "run_type" commandline argument. The Input module can
also pull USGS gage data which is dictated by the "pull" command line argument.

Command line arguments:
run_type (required): values should be "constrained" or "unconstrained"
"""

# Standard imports
from datetime import datetime
from pathlib import Path
import sys

# Local imports
from input.Extract import Extract
from input.Login import Login
from input.Write import Write

OUTPUT = Path("/mnt/data")

def main():

    # Command line arguments
    try:
        run_type = sys.argv[1]
        print(f"Running on '{run_type}' data product.")
    except IndexError:
        print("Error: No run type provided; please provide a run type argument.")
        print("Program exit.")
        sys.exit(1)
    
    # Login
    print("Logging into AWS infrastructure.")
    login = Login()
    login.login()

    # Extract SWOT data
    print("Extracting SWOT data.")
    ext = Extract()
    ext.extract_data(login.confluence_fs)
    
    # Write SWOT data
    print("Writing SWOT data to NetCDF.")
    write = Write(ext.node_data, ext.reach_data, OUTPUT)
    write.write_data()

    # Download SOS to local storage
    print("Downloading SoS.")
    write.copy_sos_data(login.confluence_fs, run_type)
    
    print("Input operations complete.")

if __name__ == "__main__":
    
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")