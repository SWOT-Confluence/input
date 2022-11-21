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
import zipfile

# Third-party imports
import pandas as pd
import numpy as np
import shapefile

# Local imports
from input.extract.ExtractStrategy import ExtractStrategy
from input.extract.exceptions import ReachNodeMismatch

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
    
    # Constants
    REACH_VARS = ["slope2", "slope2_u", "width", "width_u", "wse", "wse_u", "d_x_area", "d_x_area_u", "reach_q", "dark_frac", "ice_clim_f", "ice_dyn_f", "partial_f", "n_good_nod", "obs_frac_n", "xovr_cal_q", "time", "time_str"]
    NODE_VARS = ["width", "width_u", "wse", "wse_u", "node_q", "dark_frac", "ice_clim_f", "ice_dyn_f", "partial_f", "n_good_pix", "xovr_cal_q", "time", "time_str"]
    FLOAT_FILL = -999999999999
    
    def __init__(self, swot_id, shapefiles, cycle_pass, creds, node_ids):
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
        node_ids: list
            list of node identifiers that are associated with reach identifier
        """
        
        super().__init__(swot_id, shapefiles, cycle_pass, creds)
        self.node_ids = np.array(node_ids)
        self.data = {
            "reach": { key: np.array([]) for key in self.REACH_VARS },
            "node": None
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
        """Extracts data from SWOT shapefiles and stores in data dictionaries."""
        
        # Extract reach data
        rch_shpfile = [ shpfile for shpfile in self.shapefiles if "Reach" in shpfile ]
        for shpfile in rch_shpfile:
            df = self.get_df(shpfile)
            extracted = self.extract_reach(df)
            if extracted:
                c = Path(shpfile).name.split('_')[5]
                p = Path(shpfile).name.split('_')[6]
                self.obs_times.append(self.cycle_pass[f"{c}_{p}"])
        
        # Extract node data based on the number of observations found for reach
        node_shpfile = [ shpfile for shpfile in self.shapefiles if "Node" in shpfile ]
        self.data["node"] = create_node_dict(self.node_ids.shape[0], len(self.obs_times))
        t = 0
        for shpfile in node_shpfile:
            df = self.get_df(shpfile)
            extracted = self.extract_node(df, t)
            if extracted:
                t += 1
                c = Path(shpfile).name.split('_')[5]
                p = Path(shpfile).name.split('_')[6]
                if not self.cycle_pass[f"{c}_{p}"] in self.obs_times: raise ReachNodeMismatch
            
        # Calculate d_x_area
        if np.all((self.data["reach"]["d_x_area"] == 0)):
            self.data["reach"]["d_x_area"] = calculate_d_x_a(self.data["reach"]["wse"], self.data["reach"]["width"])    # Temp calculation of dA for current dataset
        
        # Append slope and d_x_area to node level
        self.append_node("slope2", self.node_ids.shape[0])
        self.append_node("slope2_u", self.node_ids.shape[0])
        self.append_node("d_x_area", self.node_ids.shape[0])
        self.append_node("d_x_area_u", self.node_ids.shape[0])
        
    def extract_node(self, df, t):
        """Extract node level data from shapefile found at node_file path.
    
        Parameters
        ----------
        df: Pandas.Dataframe
            Dataframe of SWOT data.
        t: int
            Current time step
        """

        # Get node identifiers for reach in dataframe
        df = df[df["node_id"].isin(self.node_ids)]
        if not df.empty:
            # Get a indexes of nodes in sorted dataframe
            df = df.sort_values(by=["node_id"], inplace=False)
            nx = np.searchsorted(self.node_ids, df["node_id"].tolist())
            for var in self.NODE_VARS:
                self.data["node"][var][nx,t] = df[var].to_numpy()
            return True
        else:
            return False       
                
    def extract_reach(self, df):
        """Extract reach level data from shapefile found at reach_file path.
    
        Parameters
        ----------
        df: Pandas.DataFrame
            dataframe of reach data
            
        Returns
        -------
        boolean indicator of data found for reach
        """
        
        # Load and locate reach identifier data
        df["reach_id"] = df["reach_id"].astype("string")
        df = df.loc[df["reach_id"] == self.swot_id]
        if not df.empty:
            # Append data into dictionary numpy arrays
            for var in self.REACH_VARS:
                self.data["reach"][var] = np.append(self.data["reach"][var], df[var])
            return True
        else:
            return False
    
    def get_df(self, shpfile):
        """Return a dataframe of SWOT data from shapefile."""
        
        # Locate and open DBF file            
        zip_file = zipfile.ZipFile(shpfile, 'r')
        dbf_file = f"{shpfile.split('/')[-1].split('.')[0]}.dbf"
        with zip_file.open(dbf_file) as dbf:
            sf = shapefile.Reader(dbf=dbf)
            fieldnames = [f[0] for f in sf.fields[1:]]
            records = sf.records()
            df = pd.DataFrame(columns=fieldnames, data=records)
        return df
                
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
        "time_str": np.full((nx, nt), np.nan, dtype="S20")
    }