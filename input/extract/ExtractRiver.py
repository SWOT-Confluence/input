"""ExtractRiver module: Contains a class for extracting shapefile data and storing
it in Numpy arrays organized by reach identifier.

Class
-----
Extract
    A class that extracts and concatenates SWOT observations from shapefiles.

Functions
---------
calculate_d_x_a(wse, width)
    Calculate and return the change in area.
create_node_dict(nx, nt)
    Initialize an empty node dict of numpy array values
"""

# Standard imports
import glob
from pathlib import Path

# Third-party imports
import geopandas as gpd
import numpy as np

# Local imports
from input.extract.ExtractStrategy import ExtractStrategy

# Constants
CONT_LOOKUP = { 1: "AF", 2: "EU", 3: "AS", 4: "AS", 5: "OC", 6: "SA", 
                    7: "NA", 8: "NA", 9: "NA"}

# Class
class ExtractRiver(ExtractStrategy):
    """A class that extends ExtractStrategy to extract river data.

    Attributes
    ----------
    data: dict
        dictionary of reach and node dictionaries
    node_ids: list
        list of integer node identifiers
    NODE_VARS: list
        list of node variables to extract from SWOT shapefiles
    reach_id: int
        integer reach identifer
    REACH_VARS: list
        list of reach variables to extract from SWOT shapefiles

    Methods
    -------
    append_node(nx, nt)
        appends reach level data to the node level    # FLAGGED AS CLASS METHOD
    extract()
        extracts data from S3 bucket shapefiles and stores in data dictionaries
    extract_node(node_file, time)
        extract node level data from shapefile found at node_file path.
    extract_reach(reach_file, time)
        extract reach level data from shapefile found at reach_file path.
    retrieve_swot_files(c_id)
        retrieve SWOT Lake shapefiles
    """
    
    LOCAL_INPUT = Path("/mnt/data/shapefiles/swot/river")    # local
    LOCAL_INPUT = Path("/home/nikki/Documents/confluence/workspace/input/data/shapefiles/swot/river")
    REACH_VARS = ["slope2", "slope2_u", "width", "width_u", "wse", "wse_u", "d_x_area", "d_x_area_u", "reach_q", "dark_frac", "ice_clim_f", "ice_dyn_f", "partial_f", "n_good_nod", "obs_frac_n", "xovr_cal_q", "time", "time_str"]
    NODE_VARS = ["width", "width_u", "wse", "wse_u", "node_q", "dark_frac", "ice_clim_f", "ice_dyn_f", "partial_f", "n_good_pix", "xovr_cal_q", "time", "time_str"]
    
    def __init__(self, confluence_fs, reach_id, node_ids):
        """
        Parameters
        ----------
        confluence_fs: S3FileSystem
            references Confluence S3 buckets
        reach_id: str
            string reach identifier
        node_ids: list
            list of string node identifiers
        """
        
        super().__init__(confluence_fs, reach_id)
        self.reach_id = reach_id
        self.node_ids = np.array(node_ids, dtype=str)  
        self.data = {
            "reach": { key: np.array([]) for key in self.REACH_VARS },
            "node": {}
        }   

    def append_node(self, key, nx):
        """Appends reach level data identified by key to the node level.
        
        Does not take into account nodes that were not observed.
        
        Parameters
        ----------
        key: str
            Name of reach and node dictionary key to append data to
        nx: int
            Number of nodes
        """

        node_data = np.tile(self.data["reach"][key], (nx, 1))
        self.data["node"][key] = node_data        

    def extract(self):
        """Extracts data from confluence_fs S3 bucket and stores in data dict.
        
        Assumes that nodes will have the same cycle and pass number as the
        reach.
        
        ## TODO: 
        - Implement PO.DAAC data extraction and storage
        """

        # Extract reach data
        cycles = list(self.cycle_data.keys())
        cycles.sort()
        for c in cycles:
            for p in self.cycle_data[c]:
                reach_file = self.confluence_fs.glob(f"confluence-swot/*_reach_{c}_{p}_*.shp")[0]
                extracted = self.extract_reach(reach_file)
                if extracted: self.obs_times.append(f"{c}/{p}")
        
        # Extract node data
        self.data["node"] = create_node_dict(self.node_ids.shape[0], len(self.obs_times))       
        for t in range(len(self.obs_times)):
            c = self.obs_times[t].split('/')[0]
            p = self.obs_times[t].split('/')[1]
            node_file = self.confluence_fs.glob(f"confluence-swot/*_node_{c}_{p}_*.shp")[0]
            self.extract_node(node_file, t)
            
        # Calculate d_x_area
        if np.all((self.data["reach"]["d_x_area"] == 0)):
            self.data["reach"]["d_x_area"] = calculate_d_x_a(self.data["reach"]["wse"], self.data["reach"]["width"])    # Temp calculation of dA for current dataset
            
        # Append slope and d_x_area to node level
        self.append_node("slope2", self.node_ids.shape[0])
        self.append_node("slope2_u", self.node_ids.shape[0])
        self.append_node("d_x_area", self.node_ids.shape[0])
        self.append_node("d_x_area_u", self.node_ids.shape[0])
    
    def extract_local(self):
        """Extracts data from SWOT shapefiles and stores in data dictionaries."""
        
        # Extract reach data
        cycles = list(self.cycle_data.keys())
        cycles.sort()
        for c in cycles:
            for p in self.cycle_data[c]:
                reach_file = Path(glob.glob(str(self.LOCAL_INPUT / f"*_reach_{c}_{p}_*.shp"))[0])
                extracted = self.extract_reach(reach_file)
                if extracted: self.obs_times.append(f"{c}/{p}")
        
        # Extract node data
        self.data["node"] = create_node_dict(self.node_ids.shape[0], len(self.obs_times))       
        for t in range(len(self.obs_times)):
            c = self.obs_times[t].split('/')[0]
            p = self.obs_times[t].split('/')[1]
            node_file = Path(glob.glob(str(self.LOCAL_INPUT / f"*_node_{c}_{p}_*.shp"))[0])
            self.extract_node(node_file, t)
            
        # Calculate d_x_area
        if np.all((self.data["reach"]["d_x_area"] == 0)):
            self.data["reach"]["d_x_area"] = calculate_d_x_a(self.data["reach"]["wse"], self.data["reach"]["width"])    # Temp calculation of dA for current dataset
        
        # Append slope and d_x_area to node level
        self.append_node("slope2", self.node_ids.shape[0])
        self.append_node("slope2_u", self.node_ids.shape[0])
        self.append_node("d_x_area", self.node_ids.shape[0])
        self.append_node("d_x_area_u", self.node_ids.shape[0])
        
    def extract_node(self, node_file, time):
        """Extract node level data from shapefile found at node_file path.
    
        Parameters
        ----------
        node_file: str
            Path to node shapefile
        time: int
            Current time step
        """
        
        # Load and locate reach identifier data
        # df = gpd.read_file(f"s3://{node_file}")
        df = gpd.read_file(node_file)    # local
        
        # Get node identifiers for reach in dataframe
        df = df[df["node_id"].isin(self.node_ids)]
        if not df.empty:
            # Get a indexes of nodes in sorted dataframe
            df.sort_values(by=["node_id"], inplace=True)
            nx = np.searchsorted(self.node_ids, df["node_id"].tolist())
            for var in self.NODE_VARS:
                self.data["node"][var][nx,time] = df[var].tolist()             
                
    def extract_reach(self, reach_file):
        """Extract reach level data from shapefile found at reach_file path.
    
        Parameters
        ----------
        reach_file: Path
            Path to reach shapefile  
            
        Returns
        -------
        boolean indicator of data found for reach
        """
        
        # Load and locate reach identifier data
        # df = gpd.read_file(f"s3://{reach_file}")
        df = gpd.read_file(reach_file)    # local
        df = df.loc[df["reach_id"] == self.reach_id]
        if not df.empty:
            # Append data into dictionary numpy arrays
            for var in self.REACH_VARS:
                self.data["reach"][var] = np.append(self.data["reach"][var], df[var])
            return True
        else:
            return False
        
    def retrieve_swot_files(self, c_id):
        """Retrieve SWOT Lake shapefiles.
        
        Parameters
        ----------
        c_id: int
            Continent integer identifier
        """
        
        c_abr = self.CONT_LOOKUP[c_id]
        c_files = [Path(c_file).name for c_file in self.confluence_fs.glob(f"confluence-swot/*reach*{c_abr}*.shp")]
        return c_files
    
    def retrieve_swot_files_local(self, c_id):
        """Retrieve SWOT Lake shapefiles.
        
        Parameters
        ----------
        c_id: int
            Continent integer identifier
        """
        
        c_abr = self.CONT_LOOKUP[c_id]
        c_files = [Path(c_file).name for c_file in glob.glob(str(self.LOCAL_INPUT / f"*reach*{c_abr}*.shp"))]
        return c_files
                
                
# Functions
def calculate_d_x_a(wse, width):
    """Calculate and return the change in area.
    
    Parameters
    ----------
    wse: numpy.ndarray
        Numpy array of wse (height) values
    width: numpy.ndarray
        Numpy array of width data
    """
    
    dH = np.subtract(wse, np.median(wse))
    return np.multiply(width, dH)

def create_node_dict(nx, nt):
    """Initialize an empty node dict of numpy values.
    
    Parameters
    ----------
    nx: int
        integer number of nodes
    nt: int
        integer number of time steps
    """

    return {
        "slope2" : np.full((nx, nt), np.nan, dtype=np.float64),
        "slope2_u" : np.full((nx, nt), np.nan, dtype=np.float64),
        "width" : np.full((nx, nt), np.nan, dtype=np.float64),
        "width_u": np.full((nx, nt), np.nan, dtype=np.float64),
        "wse" : np.full((nx, nt), np.nan, dtype=np.float64),
        "wse_u" : np.full((nx, nt), np.nan, dtype=np.float64),
        "d_x_area": np.full((nx, nt), np.nan, dtype=np.float64),
        "d_x_area_u": np.full((nx, nt), np.nan, dtype=np.float64),
        "node_q" : np.full((nx, nt), -999, dtype=int),
        "dark_frac" : np.full((nx, nt), np.nan, dtype=np.float64),
        "ice_clim_f" : np.full((nx, nt), -999, dtype=int),
        "ice_dyn_f" : np.full((nx, nt), -999, dtype=int),
        "partial_f" : np.full((nx, nt), -999, dtype=int),
        "n_good_pix" : np.full((nx, nt), -99999999, int),
        "xovr_cal_q" : np.full((nx, nt), -999, int),
        "time": np.full((nx, nt), np.nan, dtype=np.float64),
        "time_str": np.full((nx, nt), np.nan, dtype=str)
    }