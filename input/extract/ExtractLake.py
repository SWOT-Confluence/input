"""ExtractLake module: Contains a class for extracting shapefile data and storing
it in Numpy arrays organized by lake identifier.

Class
-----
Extract
    A class that extracts and concatenates SWOT observations from shapefiles.  
"""

# Standard imports
from pathlib import Path

# Local imports
from input.extract.ExtractStrategy import ExtractStrategy

# Third-party imports
import numpy as np
import shapefile

class ExtractLake(ExtractStrategy):
    """A class that extends ExtractStrategy to extract lake data.
    
    Attributes
    ----------
    data: dict
        dictionary with variable keys and numpy array value of SWOT lake data
    lake_id: int
        integer lake identifier
    LAKE_VARS: list
        list of lake variable to extract from SWOT shapefiles
        
    Methods
    -------
    extract()
        extracts data from S3 bucket shapefiles and stores in data dictionaries
    extract_lake(lake_file)
        extract lake data from lake_file SWOT shapefile
    """
    
    LAKE_VARS = ["lake_id", "time_str", "delta_s_q"]
    
    def __init__(self, swot_id, shapefiles, cycle_pass, creds):
        """
        Parameters
        ----------
        swot_id: int
            unique SWOT identifier (identifies continent)
        shapefiles: list
            list of SWOT shapefiles
        cycle_pass: dict
            dictionary of cycle pass data
        creds: dict
            dictionary of AWS S3 credentials
        """
        
        super().__init__(swot_id, shapefiles, cycle_pass, creds)
        self.data = { key: np.array([]) for key in self.LAKE_VARS }
        
    def extract(self):
        """Extracts data from SWOT shapefiles and stores in data dictionaries."""
        
        for shpfile in self.shapefiles:
            if self.creds:
                df = self.get_fsspec(shapefile)
            else:
                dbf = f"{shpfile.split('/')[-1].split('.')[0]}.dbf"
                df = self.get_df(shpfile, dbf)
            extracted = self.extract_lake(df)
            if extracted:
                c = Path(shpfile).name.split('_')[5]
                p = Path(shpfile).name.split('_')[6]
                self.obs_times.append(self.cycle_pass[f"{c}_{p}"])
                
    def extract_lake(self, df):
        """Extract lake data from lake_file SWOT shapefile.
        
        Parameters
        ----------
        df: Pandas.DataFrame
            dataframe of reach data
            
        Returns
        -------
        boolean indicator of data found for reach
        """
        
        df["lake_id"] = df["lake_id"].astype("string")
        df = df.loc[df["lake_id"] == self.swot_id]
        if not df.empty:
            # Append data into dictionary numpy arrays
            for var in self.LAKE_VARS:
                self.data[var] = np.append(self.data[var], df[var])
            return True
        else:
            return False