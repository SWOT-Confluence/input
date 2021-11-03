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
create_node_dict(node_ids)
    Initialize an empty node dict of dataframe values and reach/node ids
create_reach_dict(reach_ids)
    Initialize an empty reach dict of dataframe values and reach ids
extract_node_local(node_path, time, node_dict, sac_node)
    Extracts node data from file at node_path and stores in node_dict.
extract_reach_local(reach_path, time, reach_dict, sac_reach)
    Extracts reach data from file at reach_path and stores in reach_dict.
"""

# Standard imports
from datetime import date
import json
from pathlib import Path

# Third-party imports
import geopandas as gpd
import numpy as np
import pandas as pd

class Extract:
    """A class that extracts and concatenates SWOT observations from shapefiles.
    
    Time series data is concatenated over shapefiles.

    Attributes
    ----------
    FLOAT_FILL: float
        value to use when missing or invalid data is encountered for float
    node_data: dict
        dictionary with continent keys and dataframe value of SWOT node data
    NT: int
        number of time steps
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

    TIME_DICT = { "109": date(2009,1,9), "110": date(2009,1,10),
            "119": date(2009,1,19), "130": date(2009,1,30), "131": date(2009,1,31),
            "209": date(2009,2,9), "220": date(2009,2,20), "221": date(2009,2,21),
            "302": date(2009,3,2), "313": date(2009,3,13), "314": date(2009,3,14),
            "323": date(2009,3,23), "403": date(2009,4,3), "404": date(2009,4,4),
            "413": date(2009,4,13), "424": date(2009,4,24), "425": date(2009,4,25),
            "504": date(2009,5,4), "515": date(2009,5,15), "516": date(2009,5,16),
            "525": date(2009,5,25), "605": date(2009,6,5), "606": date(2009,6,6),
            "615": date(2009,6,15), "626": date(2009,6,26)           
    }

    # TIME_DICT = { "109": date(2009,1,9), "110": date(2009,1,10),
    #         "119": date(2009,1,19), "130": date(2009,1,30), "131": date(2009,1,31),
    #         "209": date(2009,2,9), "220": date(2009,2,20), "221": date(2009,2,21),
    #         "302": date(2009,3,2), "313": date(2009,3,13), "314": date(2009,3,14),
    #         "323": date(2009,3,23)           
    # }

    def __init__(self):
        self.reach_data = { "af": {}, "eu": {}, "si": {}, "as": 
            {}, "au": {}, "sa": {}, "na": {}, "ar": {}, "gr": {} }
        self.node_data = { "af": {}, "eu": {}, "si": {}, "as": 
            {}, "au": {}, "sa": {}, "na": {}, "ar": {}, "gr": {} }

    def append_node(self, key, nt):
        """Appends reach level data identified by key to the node level.
        
        Parameters
        ----------
        key: str
            Name of reach and node dictionary key to append data to
        nt: int
            Number of time steps (observations)
        """

        # Reach data
        r_df = self.reach_data["na"][key]
        r_ids = list(r_df.index)

        # Node data initialization
        n_df = self.node_data["na"][key]
        nan = np.full((self.node_data["na"][key].shape[0], nt), fill_value=np.nan)
        nan_df = pd.DataFrame(nan, index=self.node_data["na"][key].index, columns=range(nt))
        n_df = pd.concat([n_df,nan_df], axis=1)

        # Append reach to node
        for r_id in r_ids:
            r_data = r_df.loc[r_id]        
            n_df.loc[n_df["reach_id"] == r_id, 0:] = r_data.values

        self.node_data["na"][key] = n_df

    def extract_data(self, confluence_fs):
        """Extracts data from swot_fs S3 bucket files and stores in data dict.

        Parameters
        ----------
        confluence_fs: S3FileSystem
            references Confluence S3 buckets
        
        ## TODO: 
        - Implement PO.DAAC data extraction and storage
        """

        # Reach and node identifier data
        with open(Path(__file__).parent / "data" / "sac.json") as f:
            sac_data = json.load(f)

        # S3 SWOT files
        pass_dirs = confluence_fs.ls("confluence-swot")
        date_dirs = [ j.split('/')[2] for i in pass_dirs for j in confluence_fs.ls(i) ]
        date_dirs = sorted(date_dirs)
               
        # Extract and build reach and node dataframes
        time = 0
        reach_dict = create_reach_dict(sac_data["sac_reaches"])
        node_dict = create_node_dict(sac_data["sac_nodes"])
        for d in date_dirs:
            reach_file = confluence_fs.glob(f"confluence-swot/*/{d}/riverobs_nominal_20201105/river_data/reaches.shp")[0]
            extract_reach(reach_file, reach_dict, time)
            node_file = confluence_fs.glob(f"confluence-swot/*/{d}/riverobs_nominal_20201105/river_data/nodes.shp")[0]
            extract_node(node_file, node_dict, time)
            time += 1
        self.reach_data["na"] = reach_dict
        self.node_data["na"] = node_dict

        # Append reach d_x_area and slope2 to the node level
        self.append_node("d_x_area", time)
        self.append_node("d_x_area_u", time)
        self.append_node("slope2", time)
        self.append_node("slope2_u", time)

def calculate_d_x_a(wse, width):
    """Calculate and return the change in area.
    
    Parameters
    ----------
    wse: pandas.core.series.Series
        Series of water surface elevation data
    width: pandas.core.series.Series
        Series of width data
    """

    dH = wse.subtract(wse.median(skipna=True))
    return width.multiply(dH)

def create_node_dict(node_ids):
        """Initialize an empty node dict of dataframe values and reach/node ids.
        
        Parameters
        ----------
        node_ids: dict
            dictionary with node id keys and reach id values
        """

        df = pd.DataFrame(data=node_ids.keys(), columns=["node_id"]).set_index("node_id")
        df.insert(loc=0, column="reach_id", value=node_ids.values())
        return {
            "slope2" : df.copy(deep=True),
            "slope2_u" : df.copy(deep=True),
            "width" : df.copy(deep=True),
            "width_u": df.copy(deep=True),
            "wse" : df.copy(deep=True),
            "wse_u" : df.copy(deep=True),
            "d_x_area": df.copy(deep=True),
            "d_x_area_u": df.copy(deep=True),
            "node_q" : df.copy(deep=True),
            "dark_frac" : df.copy(deep=True),
            "ice_clim_f" : df.copy(deep=True),
            "ice_dyn_f" : df.copy(deep=True),
            "partial_f" : df.copy(deep=True),
            "n_good_pix" : df.copy(deep=True),
            "xovr_cal_q" : df.copy(deep=True)
        }

def create_reach_dict(reach_ids):
    """Initialize an empty reach dict of dataframe values and reach ids.
    
    Parameters
    ----------
    reach_ids: list
        list of reach identifiers
    """

    df = pd.DataFrame(data=reach_ids, columns=["reach_id"]).set_index("reach_id")
    return {
        "slope2" : df.copy(deep=True),
        "slope2_u": df.copy(deep=True),
        "width" : df.copy(deep=True),
        "width_u": df.copy(deep=True),
        "wse" : df.copy(deep=True),
        "wse_u" : df.copy(deep=True),
        "d_x_area": df.copy(deep=True),
        "d_x_area_u": df.copy(deep=True),
        "reach_q" : df.copy(deep=True),
        "dark_frac" : df.copy(deep=True),
        "ice_clim_f" : df.copy(deep=True),
        "ice_dyn_f" : df.copy(deep=True),
        "partial_f" : df.copy(deep=True),
        "n_good_nod" : df.copy(deep=True),
        "obs_frac_n" : df.copy(deep=True),
        "xovr_cal_q" : df.copy(deep=True),
        "time": []
    }

def extract_node(node_file, node_dict, time):
    """Extract node level data from shapefile found at node_path.
    
    Parameters
    ----------
    node_file: str
        Path to node shapefile
    node_dict: dict
        Dictionary of node data   
    time: int
        Current time step 
    """

    df = gpd.read_file(f"s3://{node_file}")

    width = df[["node_id", "width"]].rename(columns={"width": time}).set_index("node_id")
    width[time].mask(np.isclose(width[time].values, -1.00000000e+12), inplace=True)
    node_dict["width"] = node_dict["width"].join(width)

    width_u = df[["node_id", "width_u"]].rename(columns={"width_u": time}).set_index("node_id")
    width_u[time].mask(np.isclose(width_u[time].values, -1.00000000e+12), inplace=True)
    node_dict["width_u"] = node_dict["width_u"].join(width_u)

    wse = df[["node_id", "wse"]].rename(columns={"wse": time}).set_index("node_id")
    wse[time].mask(np.isclose(wse[time].values, -1.00000000e+12), inplace=True)
    node_dict["wse"] = node_dict["wse"].join(wse)

    wse_u = df[["node_id", "wse_r_u"]].rename(columns={"wse_r_u": time}).set_index("node_id")
    wse_u[time].mask(np.isclose(wse_u[time].values, -1.00000000e+12), inplace=True)
    node_dict["wse_u"] = node_dict["wse_u"].join(wse_u)

    node_q = df[["node_id", "node_q"]].rename(columns={"node_q": time}).set_index("node_id")
    node_dict["node_q"] = node_dict["node_q"].join(node_q)

    dark_frac = df[["node_id", "dark_frac"]].rename(columns={"dark_frac": time}).set_index("node_id")
    dark_frac[time].mask(np.isclose(dark_frac[time].values, -1.00000000e+12), inplace=True)
    node_dict["dark_frac"] = node_dict["dark_frac"].join(dark_frac)

    ice_clim_f = df[["node_id", "ice_clim_f"]].rename(columns={"ice_clim_f": time}).set_index("node_id")
    ice_clim_f[time].replace(-999, np.nan, inplace=True)
    node_dict["ice_clim_f"] = node_dict["ice_clim_f"].join(ice_clim_f)

    ice_dyn_f = df[["node_id", "ice_dyn_f"]].rename(columns={"ice_dyn_f": time}).set_index("node_id")
    ice_dyn_f[time].replace(-999, np.nan, inplace=True)
    node_dict["ice_dyn_f"] = node_dict["ice_dyn_f"].join(ice_dyn_f)

    partial_f = df[["node_id", "partial_f"]].rename(columns={"partial_f": time}).set_index("node_id")
    partial_f[time].replace(-999, np.nan, inplace=True)
    node_dict["partial_f"] = node_dict["partial_f"].join(partial_f)

    n_good_pix = df[["node_id", "n_good_pix"]].rename(columns={"n_good_pix": time}).set_index("node_id")
    n_good_pix[time].replace(-999, np.nan, inplace=True)
    node_dict["n_good_pix"] = node_dict["n_good_pix"].join(n_good_pix)

    xovr_cal_q = df[["node_id", "xovr_cal_q"]].rename(columns={"xovr_cal_q": time}).set_index("node_id")
    xovr_cal_q[time].replace(-999, np.nan, inplace=True)
    node_dict["xovr_cal_q"] = node_dict["xovr_cal_q"].join(xovr_cal_q)

def extract_reach(reach_file, reach_dict, time):
    """Extract reach level data from shapefile found at reach_path.
    
    Parameters
    ----------
    reach_file: str
        Path to reach shapefile
    reach_dict: dict
        Dictionary of reach data   
    time: int
        Current time step 
    """
    
    df = gpd.read_file(f"s3://{reach_file}")
    
    slope = df[["reach_id", "slope2"]].rename(columns={"slope2": time}).set_index("reach_id")
    slope[time].mask(np.isclose(slope[time].values, -1.00000000e+12), inplace=True)
    reach_dict["slope2"] = reach_dict["slope2"].join(slope)

    slope_u = df[["reach_id", "slope2_u"]].rename(columns={"slope2_u": time}).set_index("reach_id")
    slope_u[time].mask(np.isclose(slope_u[time].values, -1.00000000e+12), inplace=True)
    reach_dict["slope2_u"] = reach_dict["slope2_u"].join(slope_u)

    width = df[["reach_id", "width"]].rename(columns={"width": time}).set_index("reach_id")
    width[time].mask(np.isclose(width[time].values, -1.00000000e+12), inplace=True)
    reach_dict["width"] = reach_dict["width"].join(width)

    width_u = df[["reach_id", "width_u"]].rename(columns={"width_u": time}).set_index("reach_id")
    width_u[time].mask(np.isclose(width_u[time].values, -1.00000000e+12), inplace=True)
    reach_dict["width_u"] = reach_dict["width_u"].join(width_u)

    wse = df[["reach_id", "wse"]].rename(columns={"wse": time}).set_index("reach_id")
    wse[time].mask(np.isclose(wse[time].values, -1.00000000e+12), inplace=True)
    reach_dict["wse"] = reach_dict["wse"].join(wse)

    wse_u = df[["reach_id", "wse_r_u"]].rename(columns={"wse_r_u": time}).set_index("reach_id")
    wse_u[time].mask(np.isclose(wse_u[time].values, -1.00000000e+12), inplace=True)
    reach_dict["wse_u"] = reach_dict["wse_u"].join(wse_u)

    d_x_area = calculate_d_x_a(wse[time], width[time]).rename(time)
    reach_dict["d_x_area"] = reach_dict["d_x_area"].join(d_x_area)
    
    # d_x_area = df[["reach_id", "d_x_area"]].rename(columns={"d_x_area": time}).set_index("reach_id")
    # d_x_area[time].mask(np.isclose(d_x_area[time].values, -1.00000000e+12), inplace=True)
    # d_x_area[time].mask(np.isclose(d_x_area[time].values, -9999.0), inplace=True)
    # reach_dict["d_x_area"] = reach_dict["d_x_area"].join(d_x_area)

    d_x_area_u = df[["reach_id", "d_x_area_u"]].rename(columns={"d_x_area_u": time}).set_index("reach_id")
    d_x_area_u[time].mask(np.isclose(d_x_area_u[time].values, -1.00000000e+12), inplace=True)
    reach_dict["d_x_area_u"] = reach_dict["d_x_area_u"].join(d_x_area_u)

    reach_q = df[["reach_id", "reach_q"]].rename(columns={"reach_q": time}).set_index("reach_id")
    reach_dict["reach_q"] = reach_dict["reach_q"].join(reach_q)

    dark_frac = df[["reach_id", "dark_frac"]].rename(columns={"dark_frac": time}).set_index("reach_id")
    dark_frac[time].mask(np.isclose(dark_frac[time].values, -1.00000000e+12), inplace=True)
    reach_dict["dark_frac"] = reach_dict["dark_frac"].join(dark_frac)

    ice_clim_f = df[["reach_id", "ice_clim_f"]].rename(columns={"ice_clim_f": time}).set_index("reach_id")
    ice_clim_f[time].replace(-999, np.nan, inplace=True)
    reach_dict["ice_clim_f"] = reach_dict["ice_clim_f"].join(ice_clim_f)

    ice_dyn_f = df[["reach_id", "ice_dyn_f"]].rename(columns={"ice_dyn_f": time}).set_index("reach_id")
    ice_dyn_f[time].replace(-999, np.nan, inplace=True)
    reach_dict["ice_dyn_f"] = reach_dict["ice_dyn_f"].join(ice_dyn_f)

    partial_f = df[["reach_id", "partial_f"]].rename(columns={"partial_f": time}).set_index("reach_id")
    reach_dict["partial_f"] = reach_dict["partial_f"].join(partial_f)

    n_good_nod = df[["reach_id", "n_good_nod"]].rename(columns={"n_good_nod": time}).set_index("reach_id")
    n_good_nod[time].replace(-999, np.nan, inplace=True)
    reach_dict["n_good_nod"] = reach_dict["n_good_nod"].join(n_good_nod)

    obs_frac_n = df[["reach_id", "obs_frac_n"]].rename(columns={"obs_frac_n": time}).set_index("reach_id")
    obs_frac_n[time].mask(np.isclose(obs_frac_n[time].values, -1.00000000e+12), inplace=True)
    reach_dict["obs_frac_n"] = reach_dict["obs_frac_n"].join(obs_frac_n)

    xovr_cal_q = df[["reach_id", "xovr_cal_q"]].rename(columns={"xovr_cal_q": time}).set_index("reach_id")
    xovr_cal_q[time].replace(-999, np.nan, inplace=True)
    reach_dict["xovr_cal_q"] = reach_dict["xovr_cal_q"].join(xovr_cal_q)

    time_step = int(Extract.TIME_DICT[reach_file.split('/')[2]].strftime("%Y%m%d"))
    reach_dict["time"].append(time_step)