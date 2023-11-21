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
from pathlib import Path
import time

# Third-party imports
import numpy as np
import boto3
import botocore
from time import sleep
from random import randint

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
    """
    
    # Constants
    FLOAT_FILL = -999999999999
    REACH_VARS = ["slope", "slope_u", "slope2", "slope2_u", "width", "width_u", "wse", "wse_u", "d_x_area", "d_x_area_u", "reach_q", "dark_frac", "ice_clim_f", "ice_dyn_f", "partial_f", "n_good_nod", "obs_frac_n", "xovr_cal_q", "time", "time_str"]
    # NODE_VARS = ["width", "width_u", "wse", "wse_u", "node_q", "dark_frac", "ice_clim_f", "ice_dyn_f", "partial_f", "n_good_pix", "xovr_cal_q", "time", "time_str"]
    NODE_VARS = ["width", "width_u", "wse", "wse_u", "node_q", "dark_frac", "ice_clim_f", "ice_dyn_f", "node_q_b","n_good_pix", "xovr_cal_q", "time", "time_str"]
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
        print('Processing reach', swot_id)
        self.data = {
            "reach": { key: np.array([]) for key in self.REACH_VARS },
            "node": None
        }

    def get_creds(self):
        """Return AWS S3 credentials to access S3 shapefiles."""
        
        ssm_client = boto3.client('ssm', region_name="us-west-2")
        creds = {}
        retry_count = 10
        while retry_count>0:
            try:
                creds["access_key"] = ssm_client.get_parameter(Name="s3_creds_key", WithDecryption=True)["Parameter"]["Value"]
                creds["secret"] = ssm_client.get_parameter(Name="s3_creds_secret", WithDecryption=True)["Parameter"]["Value"]
                creds["token"] = ssm_client.get_parameter(Name="s3_creds_token", WithDecryption=True)["Parameter"]["Value"]
                retry_count = -999
            except:
                print('Error pulling credentials, retrying:', retry_count)
                retry_count-=1
                sleep(randint(1,300))
        if retry_count == 0:
            try:
                print('Final Try...')
                creds["access_key"] = ssm_client.get_parameter(Name="s3_creds_key", WithDecryption=True)["Parameter"]["Value"]
                creds["secret"] = ssm_client.get_parameter(Name="s3_creds_secret", WithDecryption=True)["Parameter"]["Value"]
                creds["token"] = ssm_client.get_parameter(Name="s3_creds_token", WithDecryption=True)["Parameter"]["Value"]
                retry_count = -999
            except botocore.exceptions.ClientError as e:
                raise e

        else:
            return creds


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
        mapping_dict = {}
        all_shps = []
        # Extract reach data
        rch_shpfile = [ shpfile for shpfile in self.shapefiles if "Reach" in shpfile ]
        print('Pulling reach files...')
        #timing and re-up creds every 30 mins
        start = time.time()
        for shpfile in rch_shpfile:
            # make sure it is first processing
            if shpfile[-5] == '1':
                if self.creds: 
                    df = self.get_fsspec(shpfile)
                else:
                    dbf = f"{shpfile.split('/')[-1].split('.')[0]}.dbf"
                    df = self.get_df(shpfile, dbf)
                    
                extracted = self.extract_reach(df)
                if extracted:
                    all_shps.append(shpfile)
                    c = Path(shpfile).name.split('_')[5]
                    p = Path(shpfile).name.split('_')[6]
                    self.obs_times.append(self.cycle_pass[f"{c}_{p}"])
                end = time.time()
                time_delta = end-start
                if time_delta > 1800:
                    self.creds = self.get_creds()
                    creds = self.creds
                    start = time.time()

        mapping_dict[self.swot_id] = all_shps
        import json
        with open(f'/mnt/data/swot/creation_logs/{self.swot_id}.json', 'w') as fp:
            json.dump(mapping_dict, fp)
        self.obs_times = list(set(self.obs_times))
        # Extract node data based on the number of observations found for reach
        node_shpfile = [ shpfile for shpfile in self.shapefiles if "Node" in shpfile ]
        self.data["node"] = create_node_dict(self.node_ids.shape[0], len(self.obs_times))
        t = 0 # this and obs time off, check shape file if there is error
        # map out node shapefiles as well
        #is there a cas where there is a reach shapefile and not a node shapefile


        for shpfile in node_shpfile:
            # check if it is the first processing
            if shpfile[-5] == '1':

                if self.creds: 
                    # print(shpfile)
                    df = self.get_fsspec(shpfile)
                else:
                    dbf = f"{shpfile.split('/')[-1].split('.')[0]}.dbf"
                    df = self.get_df(shpfile, dbf)
                extracted = self.extract_node(df, t)
                if extracted:
                    t += 1
                    c = Path(shpfile).name.split('_')[5]
                    p = Path(shpfile).name.split('_')[6]
                    if not self.cycle_pass[f"{c}_{p}"] in self.obs_times:
                        print('Error we are working on...')
                        print(f"{c}_{p}")
                        print('error testing')
                        print('node', self.cycle_pass[f"{c}_{p}"])
                        print('reach', self.swot_id )
                        for i in self.obs_times:
                            print(i)
                        raise ReachNodeMismatch
                end = time.time()
                time_delta = end-start
                if time_delta > 1800:
                    self.creds = self.get_creds()
                    creds = self.creds
                    start = time.time()
            
        # Calculate d_x_area
        if np.all((self.data["reach"]["d_x_area"] == self.FLOAT_FILL)):
            self.data["reach"]["d_x_area"] = calculate_d_x_a(self.data["reach"]["wse"], self.data["reach"]["width"])    # Temp calculation of dA for current dataset
        
        # Append slope and d_x_area to node level
        self.append_node("slope", self.node_ids.shape[0])
        self.append_node("slope_u", self.node_ids.shape[0])
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
                try:
                    self.data["node"][var][nx,t] = df[var].to_numpy()
                except:
                    print('indexing error occured dimensions were', 'nx', nx, 'by nt', t)
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
        "slope" : np.full((nx, nt), np.nan, dtype=np.float64),
        "slope_u" : np.full((nx, nt), np.nan, dtype=np.float64),
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
        "node_q_b" : np.full((nx, nt), -999, dtype=int),
        "n_good_pix" : np.full((nx, nt), -99999999, int),
        "xovr_cal_q" : np.full((nx, nt), -999, int),
        "time": np.full((nx, nt), np.nan, dtype=np.float64),
        "time_str": np.full((nx, nt), np.nan, dtype="S20")
    }