"""Script to append USGS data to SoS files."""

# Standard imports
from datetime import datetime
from pathlib import Path

# Local imports 
from input.gage_pull.GageAppend import GageAppend
from input.gage_pull.GagePull import GagePull

USGS_DIR = Path("/home/nikki/Documents/confluence/data/sos/usgs")
SOS_DIR = Path("/home/nikki/Documents/confluence/data/sos/sos/constrained")

def main():
    
    # Append USGS gage data to the SoS
    print("Pulling USGS gage data and appending to SoS.")
    gage_pull = GagePull(USGS_DIR / "USGStargetsV3.nc", '1980-1-1', datetime.today().strftime("%Y-%m-%d"))
    gage_pull.pull()
    gage_append = GageAppend(SOS_DIR, gage_pull.usgs_dict)
    gage_append.read_sos()
    gage_append.map_data()
    gage_append.append_data()

    print("Input operations complete.")

if __name__ == "__main__":
    
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")