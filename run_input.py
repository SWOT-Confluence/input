# Standard imports
from os import scandir
from pathlib import Path

# Local imports
from src.Login import Login
from src.Upload import Upload
from src.Write import Write   

INPUT = ""
OUTPUT = ""

def main():
    # Login
    login = Login()
    login.login()

    # Extract and write SWOT data
    with scandir(INPUT) as entries:
        dirs = [ entry.path for entry in entries ]
    write = Write()
    write.extract_data_local(dirs)
    write.write_data()

    # Upload SWOT and SoS data
    # upload = Upload(login.sos_fs, login.swot_fs)
    upload = Upload(None, None)
    upload.upload_data_local(OUTPUT, write.temp_dir)

if __name__ == "__main__":
    main()