# Standard imports
from datetime import date
from os import scandir
from pathlib import Path

# Local imports
from src.Extract import Extract
from src.Login import Login
from src.Upload import Upload
from src.Write import Write   

INPUT = Path("")
OUTPUT = Path("")

def main():
    # Login
    # login = Login()
    # login.login()

    # Extract SWOT data
    with scandir(INPUT) as entries:
        dirs = sorted([ entry.path for entry in entries ])
    ext = Extract()
    ext.extract_data_local(dirs)
    
    # Write SWOT data
    write = Write(ext.node_data, ext.reach_data)
    write.write_data()

    # Upload SWOT and SoS data
    # upload = Upload(login.sos_fs, login.swot_fs)
    upload = Upload(None, None)
    upload.upload_data_local(OUTPUT, write.temp_dir)

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")