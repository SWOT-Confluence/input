"""ExtractLake module: Contains a class for extracting shapefile data and storing
it in Numpy arrays organized by lake identifier.

Class
-----
Extract
    A class that extracts and concatenates SWOT observations from shapefiles.

Functions
---------

    
"""

# Standard imports
import glob
from pathlib import Path

# Local imports
from input.extract.ExtractStrategy import ExtractStrategy

# Third-party imports
import geopandas as gpd
import numpy as np

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
    retrieve_swot_files(c_id)
        retrieve SWOT Lake shapefiles
    """
    
    LAKE_VARS = ["lake_id", "time_str", "delta_s_q"]
    
    def __init__(self, confluence_fs, lake_id, cycle_pass_json):
        """
        Parameters
        ----------
        confluence_fs: S3FileSystem
            references Confluence S3 buckets
        lake_id: str
            string lake identifier
        cycle_pass_json: Path
            path to cycle pass JSON file
        """
        
        super().__init__(confluence_fs, lake_id, cycle_pass_json)
        self.lake_id = lake_id
        self.data = { key: np.array([]) for key in self.LAKE_VARS }
        
    def retrieve_swot_files(self, c_id):
        """Retrieve SWOT Lake shapefiles.
        
        Parameters
        ----------
        c_id: int
            Continent integer identifier
        """
        
        c_abr = self.CONT_LOOKUP[c_id]
        c_files = [Path(c_file).name for c_file in self.confluence_fs.glob(f"confluence-swot/*Prior*{c_abr}*.shp")]
        return c_files
    
    def retrieve_swot_files_local(self, c_id):
        """Retrieve SWOT Lake shapefiles.
        
        Parameters
        ----------
        c_id: int
            Continent integer identifier
        """
        
        c_abr = self.CONT_LOOKUP[c_id]
        c_files = [Path(c_file).name for c_file in glob.glob(str(self.LOCAL_INPUT / f"*Prior*{c_abr}*.shp"))]
        return c_files
        
    def extract(self):
        """Extracts data from SWOT shapefiles and stores in data dictionaries."""
        
        cycles = list(self.cycle_data.keys())
        cycles.sort()
        for c in cycles:
            for p in self.cycle_data[c]:
                lake_file = self.confluence_fs.glob(f"confluence-swot/*_Prior_{c}_{p}_*.shp")[0]
                extracted = self.extract_lake(lake_file)
                if extracted: self.obs_times.append(self.pass_data[f"{c}_{p}"])
        
    def extract_local(self):
        """Extracts data from SWOT shapefiles and stores in data dictionaries."""
        
        cycles = list(self.cycle_data.keys())
        cycles.sort()
        for c in cycles:
            for p in self.cycle_data[c]:
                lake_file = Path(glob.glob(str(self.LOCAL_INPUT / f"*_Prior_{c}_{p}_*.shp"))[0])
                extracted = self.extract_lake(lake_file)
                if extracted: self.obs_times.append(f"{c}/{p}")
                
    def extract_lake(self, lake_file):
        """Extract lake data from lake_file SWOT shapefile.
        
        Parameters
        ----------
        lake_file: Path
            Path to lake shapefile
            
        Returns
        -------
        Returns
        -------
        boolean indicator of data found for reach
        """
        
        df = gpd.read_file(f"s3://{lake_file}")
        # df = gpd.read_file(lake_file)    # local
        df = df.loc[df["lake_id"] == self.lake_id]
        if not df.empty:
            # Append data into dictionary numpy arrays
            for var in self.LAKE_VARS:
                self.data[var] = np.append(self.data[var], df[var])
            return True
        else:
            return False