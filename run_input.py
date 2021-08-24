# Standard imports
from pathlib import Path

# Local imports
from src.Extract import Extract
from src.Login import Login
from src.Write import Write   

OUTPUT = Path("/mnt/data")

def main():
    # Login
    login = Login()
    login.login()

    # Extract SWOT data
    ext = Extract()
    ext.extract_data(login.confluence_fs)
    
    # Write SWOT data
    write = Write(ext.node_data, ext.reach_data, OUTPUT)
    write.copy_sos_data(login.confluence_fs)
    write.write_data()

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")