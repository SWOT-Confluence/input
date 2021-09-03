# Standard imports
from datetime import datetime
from pathlib import Path

# Local imports
# from src.Extract import Extract
from src.Extract_local import Extract
from src.GagePull import GagePull
from src.Login import Login
from src.Write import Write   

INPUT = Path("")
OUTPUT = Path("/home/nikki/Documents/confluence/workspace/input/data")

def main():
    # # Login
    # # login = Login()
    # # login.login()

    # # Extract SWOT data
    # ext = Extract()
    # # ext.extract_data(login.confluence_fs)
    # ext.extract_data(None)
    
    # # Copy SoS data and Write SWOT data
    # write = Write(ext.node_data, ext.reach_data, OUTPUT)
    # # write.copy_sos_data(login.confluence_fs)
    # write.write_data()

    # Append USGS gage data to the SoS
    gage_pull = GagePull(OUTPUT / "sos", OUTPUT / "usgs" / "USGStargetsV3.nc", '1980-1-1', datetime.today().strftime("%Y-%m-%d"))
    gage_pull.pull()

if __name__ == "__main__":
    
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")