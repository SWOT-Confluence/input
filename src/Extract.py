"""Extract module: Contains a class for extracting shapefile data and storing
it in Numpy arrays organized by continent.

Class
-----
Extract
    A class that extracts and concatenates SWOT observations from shapefiles.

Functions
---------
calculate_d_x_a(wse, width)
    Calculate and return the change in area.
extract_node_local(node_path, time, node_dict, sac_node)
    Extracts node data from file at node_path and stores in node_dict.
extract_reach_local(reach_path, time, reach_dict, sac_reach)
    Extracts reach data from file at reach_path and stores in reach_dict.
"""

# Standard imports
import json
from pathlib import Path

# Third-party imports
import numpy as np
import pandas as pd
import shapefile as shp

class Extract:
    """A class that extracts and concatenates SWOT observations from shapefiles.
    
    Time series data is concatenated over shapefiles.

    Attributes
    ----------
    continents: dict
        dictionary of continent IDs (keys) and continent names (values)
    FLOAT_FILL: float
        value to use when missing or invalid data is encountered for float
    node_data: dict
        dictionary with continent keys and dataframe value of SWOT node data
    reach_data: dict
        dictionary with continent keys and dataframe value of SWOT reach data

    Methods
    -------
    append_node(key):
        Appends reach level data to the node level.
    extract_data(swot_fs), not implemented
        extracts data from SWOT S3 bucket and stores in data dictionary
    extract_data_local()
        extracts data from local shapefiles
    """

    FLOAT_FILL = -999999999999

    def __init__(self):
        self.continents = { "af": "Africa", "eu": "Europe and Middle East",
            "si": "Siberia", "as": "Central and Southeast Asia",
            "au": "Australia and Oceania", "sa": "South America",
            "na": "North America and Caribbean", "ar": "North American Arctic",
            "gr": "Greenland" }
        self.reach_data = { "af": None, "eu": None, "si": None, "as": None,
            "au": None, "sa": None, "na": None, "ar": None, "gr": None }
        self.node_data = { "af": None, "eu": None, "si": None, "as": None,
            "au": None, "sa": None, "na": None, "ar": None, "gr": None }

    def append_node(self, key):
        """Appends reach level data identified by key to the node level.
        
        Parameters
        ----------
        key: str
            Name of reach and node dictionary key to append data to
        """

        reach_ids = self.reach_data["na"]["reach_id"]
        self.node_data["na"][key] = np.zeros((self.node_data["na"]["nt"], 
            self.node_data["na"]["nx"]), dtype=float)
        for reach_id in reach_ids:
            # indexes
            reach_i = np.where(self.reach_data["na"]["reach_id"] == reach_id)
            node_i = np.where(self.node_data["na"]["reach_id"] == reach_id)

            # append data
            r_data = self.reach_data["na"][key][:, reach_i].flatten('C')                
            r_data = r_data.reshape((np.size(r_data), 1))                
            n_data = np.repeat(r_data, repeats=np.size(node_i), axis=1)
            n_data = n_data.reshape((np.size(r_data), 1, np.size(node_i)))
            self.node_data["na"][key][:, node_i] = n_data

    def extract_data(self, swot_fs):
        """Extracts data from swot_fs S3 bucket files and stores in data dict.

        Parameters
        ----------
        swot_fs: S3FileSystem
            References PO.DAAC SWOT S3 bucket
        
        ## TODO: 
        - Implement
        """

        raise NotImplementedError

    def extract_data_local(self, dirs):
        """Extracts Sacramento data from directory parameter and stores in ??

        NOTE: This is for use with Sacramento data only.
        
        Parameters
        ----------
        pass_dirs: list
            list of directories that contain pass data
        """

        # Data
        with open(Path(__file__).parent / "data" / "sac.json") as f:
            sac_data = json.load(f)
        
        # Extract and concatenate data
        time = 0
        reach_dict = {}
        node_dict = {}
        for d in dirs:
            reach_path = Path(d) / "riverobs_nominal_20201105" / "river_data" / "reaches.shp"
            extract_reach_local(reach_path, time, reach_dict, sac_data["sac_reaches"])
            node_path = Path(d) / "riverobs_nominal_20201105" / "river_data" / "nodes.shp"
            extract_node_local(node_path, time, node_dict, sac_data["sac_nodes"])
            time += 1

        self.reach_data["na"] = reach_dict
        self.node_data["na"] = node_dict
        
        # Track dimensions
        self.reach_data["na"]["nt"] = time
        self.node_data["na"]["nt"] = time
        self.node_data["na"]["nx"] = np.size(self.node_data["na"]["width"], axis=1)

        # Append d_x_area and slope2 to the node level
        self.append_node("d_x_area")
        self.append_node("slope2")

def calculate_d_x_a(wse, width):
    """Calculate and return the change in area.
    
    Parameters
    ----------
    wse: np.ndarray
        Numpy array of wse data
    width: np.ndaray
        Numpy array of width data
    """
    
    wse[np.isclose(wse, -1.00000000e+12)] = np.nan
    width[np.isclose(width, -1.00000000e+12)] = np.nan
    d_h = np.subtract(wse, np.nanmedian(wse))
    return np.multiply(width, d_h)

def extract_node_local(node_path, time, node_dict, sac_node):
    """Extracts node data from file at node_path and stores in node_dict.
    
    Parameters
    ----------
    node_path: Path
        Path to node shapefile
    time: int
        Current time step
    node_dict: dict
        Dictionary of node data
    sac_node: dict
        Dictionary of reach identifiers organized by node identifier
    """

    sf = shp.Reader(str(node_path))
    fields = [x[0] for x in sf.fields][1:]
    records = sf.records()
    df = pd.DataFrame(columns=fields, data=records)

    # Add node ids that are not present in the data with missing values
    diff = np.setdiff1d(list(sac_node.keys()), df["node_id"].to_numpy())
    no_data = [np.NaN] * (df.shape[1] - 2)
    for node in diff:
        row = [sac_node[node], node]
        row.extend(no_data)
        df = df.append(pd.Series(row, index=df.columns), ignore_index=True)

    if time == 0:
        node_dict["reach_id"] = df["reach_id"].astype(int).to_numpy()
        node_dict["node_id"] = df["node_id"].astype(int).to_numpy()
        node_dict["width"] = df["width"].to_numpy()
        node_dict["wse"] = df["wse"].to_numpy()
        node_dict["node_q"] = df["node_q"].to_numpy()
        node_dict["dark_frac"] = df["dark_frac"].to_numpy()
        node_dict["ice_clim_f"] = df["ice_clim_f"].to_numpy()
        node_dict["ice_dyn_f"] = df["ice_dyn_f"].to_numpy()
        node_dict["partial_f"] = df["partial_f"].to_numpy()
        node_dict["n_good_pix"] = df["n_good_pix"].to_numpy()
        node_dict["xovr_cal_q"] = df["xovr_cal_q"].to_numpy()
    else:
        node_dict["width"] = np.vstack((node_dict["width"], df["width"].to_numpy()))
        node_dict["wse"] = np.vstack((node_dict["wse"], df["wse"].to_numpy()))
        node_dict["node_q"] = np.vstack((node_dict["node_q"], df["node_q"].to_numpy()))
        node_dict["dark_frac"] = np.vstack((node_dict["dark_frac"], df["dark_frac"].to_numpy()))
        node_dict["ice_clim_f"] = np.vstack((node_dict["ice_clim_f"], df["ice_clim_f"].to_numpy()))
        node_dict["ice_dyn_f"] = np.vstack((node_dict["ice_dyn_f"], df["ice_dyn_f"].to_numpy()))
        node_dict["partial_f"] = np.vstack((node_dict["partial_f"], df["partial_f"].to_numpy()))
        node_dict["n_good_pix"] = np.vstack((node_dict["n_good_pix"], df["n_good_pix"].to_numpy()))
        node_dict["xovr_cal_q"] = np.vstack((node_dict["xovr_cal_q"], df["xovr_cal_q"].to_numpy()))

def extract_reach_local(reach_path, time, reach_dict, sac_reach):
    """Extracts reach data from file at reach_path and stores in reach_dict.
    
    Parameters
    ----------
    reach_path: Path
        Path to reach shapefile
    time: int
        Current time step
    reach_dict: dict
        Dictionary of reach data
    sac_reach: list
        List of reach identifiers
    """

    sf = shp.Reader(str(reach_path))
    fields = [x[0] for x in sf.fields][1:]
    records = sf.records()
    df = pd.DataFrame(columns=fields, data=records)
    df.replace(to_replace=-9999, value=Extract.FLOAT_FILL, inplace=True)

    # Add reach ids that are not present in the data with missing values
    diff = np.setdiff1d(sac_reach, df["reach_id"].to_numpy())
    no_data = [np.NaN] * (df.shape[1] - 1)
    for reach in diff:
        row = [reach]
        row.extend(no_data)
        df = df.append(pd.Series(row, index=df.columns), ignore_index=True)

    if time == 0:
        reach_dict["reach_id"] = df["reach_id"].astype(int).to_numpy()
        reach_dict["slope2"] = df["slope2"].to_numpy()
        reach_dict["width"] = df["width"].to_numpy()
        reach_dict["wse"] = df["wse"].to_numpy()
        reach_dict["d_x_area"] = calculate_d_x_a(df["wse"].to_numpy(), df["width"].to_numpy())
        reach_dict["reach_q"] = df["reach_q"].to_numpy()
        reach_dict["dark_frac"] = df["dark_frac"].to_numpy()
        reach_dict["ice_clim_f"] = df["ice_clim_f"].to_numpy()
        reach_dict["ice_dyn_f"] = df["ice_dyn_f"].to_numpy()
        reach_dict["partial_f"] = df["partial_f"].to_numpy()
        reach_dict["n_good_nod"] = df["n_good_nod"].to_numpy()
        reach_dict["obs_frac_n"] = df["obs_frac_n"].to_numpy()
        reach_dict["xovr_cal_q"] = df["xovr_cal_q"].to_numpy()
    else:
        reach_dict["slope2"] = np.vstack((reach_dict["slope2"], df["slope2"].to_numpy()))
        reach_dict["width"] = np.vstack((reach_dict["width"], df["width"].to_numpy()))
        reach_dict["wse"] = np.vstack((reach_dict["wse"], df["wse"].to_numpy()))
        reach_dict["d_x_area"] = np.vstack((reach_dict["d_x_area"], calculate_d_x_a(df["wse"].to_numpy(), df["width"].to_numpy())))
        reach_dict["reach_q"] = np.vstack((reach_dict["reach_q"], df["reach_q"].to_numpy()))
        reach_dict["dark_frac"] = np.vstack((reach_dict["dark_frac"], df["dark_frac"].to_numpy()))
        reach_dict["ice_clim_f"] = np.vstack((reach_dict["ice_clim_f"], df["ice_clim_f"].to_numpy()))
        reach_dict["ice_dyn_f"] = np.vstack((reach_dict["ice_dyn_f"], df["ice_dyn_f"].to_numpy()))
        reach_dict["partial_f"] = np.vstack((reach_dict["partial_f"], df["partial_f"].to_numpy()))
        reach_dict["n_good_nod"] = np.vstack((reach_dict["n_good_nod"], df["n_good_nod"].to_numpy()))
        reach_dict["obs_frac_n"] = np.vstack((reach_dict["obs_frac_n"], df["obs_frac_n"].to_numpy()))
        reach_dict["xovr_cal_q"] = np.vstack((reach_dict["xovr_cal_q"], df["xovr_cal_q"].to_numpy()))