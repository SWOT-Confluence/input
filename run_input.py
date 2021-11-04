"""Script to run Input module.

The Input module logs into PO.DAAC AWS S3 infrastructure (TODO) and the 
Confluence S3 infrastructure. The Input module extracts SWOT observations and
formats them as one NetCDF per reach.
"""

# Standard imports
from datetime import datetime
from pathlib import Path

# Local imports
from input.Extract import Extract
from input.Login import Login
from input.Write import Write

OUTPUT = Path("/mnt/data")

def main():
    
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
    
    print("Input operations complete.")

if __name__ == "__main__":
    
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")