# Standard imports
from datetime import datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory

# Third-party imports
from netCDF4 import Dataset
import numpy as np
import pandas as pd
import shapefile as shp

class Write:
    """A class that takes SWOT and SoS data and writes intermediate input data.
    
    Intermediate data is stored in NetCDF file format and is taken from various
    AWS S3 buckets.

    Attributes
    ----------
    continents: dict
        dictionary of continent IDs (keys) and continent names (values)
    FLOAT_FILL: float
        value to use when missing or invalid data is encountered for float
    INT_FILL: int
        value to use when missing or invalid data is encountered for integers
    node_data: dict
        dictionary with continent keys and dataframe value of SWOT node data
    reach_data: dict
        dictionary with continent keys and dataframe value of SWOT reach data

    Methods
    -------
    __define_global_attrs(dataset, level, cont)
        Set global attributes for NetCDF dataset file
    extract_data()
        extracts data from SWOT S3 bucket and stores in data dictionary
    write_data()
        writes SWOT data dictionaries to NetCDF files organized by continent
    __write_data(nc_file)
        writes node level data to NetCDF file in node group
    __write_reach_data(nc_file)
        writes reach level data to NetCDF file in reach group
    """

    FLOAT_FILL = -999999999999
    INT_FILL = -999

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
        self.temp_dir = TemporaryDirectory()

    def __define_global_attrs(self, dataset, level, cont):
        """Set global attributes for NetCDF dataset file.

        Currently sets title, history, and continent.

        Parameter
        ---------
        dataset: netCDF4.Dataset
            NetCDF4 dataset to set global attirbutes for
        level: str
            Reach or Node level dataset is storing data for
        cont: str
            Continent that data was obtained for
        
        ## TODO:
        - cycle number, pass number, time_coverage (range)
        """

        dataset.title = f"SWOT {level}-Level Data"
        dataset.history = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
        dataset.continent = self.continents[cont]

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

    def extract_data_local(self, files):
        """Extracts data from files parameter and stores in data dict.

        ## TODO: Duplicate reach and node identifiers are dropped from the data
        without regards for data. This is for testing purposes only and is due
        to limited example data.
        
        Parameters
        ----------
        files: list
            list of shapefiles to extract SWOT data from
        """

        for file in files:
            sf = shp.Reader(file.path)
            fields = [x[0] for x in sf.fields][1:]
            records = sf.records()
            df = pd.DataFrame(columns=fields, data=records)
            df.replace(to_replace=-9999, value=-999999999999, inplace=True)
            df.replace(to_replace=np.nan, value=-999999999999, inplace=True)

            name_list = file.name.split('_')
            continent_id = name_list[7].lower()
            if "Reach" in name_list:
                df.drop_duplicates(subset=["reach_id"], inplace=True)
                if self.reach_data[continent_id]:
                    self.reach_data[continent_id].append(df)
                else:
                    self.reach_data[continent_id] = df
            else:
                df.drop_duplicates(subset=["node_id"], inplace=True)
                if self.node_data[continent_id]:
                    self.node_data[continent_id].append(df)
                else:
                    self.node_data[continent_id] = df

    def write_data(self):
        """Writes node and reach level SWOT data to NetCDF format.
        
        Files are placed in a temporary directory.

        ## TODO
        - remove if statement testing for None as all keys should be populated
        """

        for key in self.continents.keys():
            if self.reach_data[key] is not None and self.node_data[key] is not None:
                # Reach
                reach_file = Path(self.temp_dir.name) / f"Reach_{key.upper()}.nc"
                reach_dataset = Dataset(reach_file, 'w', format="NETCDF4")
                self.define_global_attrs(reach_dataset, "Reach", key)
                self.write_reach_data(self.reach_data[key], reach_dataset)
                reach_dataset.close()
                # Node
                node_file = Path(self.temp_dir.name) / f"Node_{key.upper()}.nc"
                node_dataset = Dataset(node_file, 'w', format="NETCDF4")
                self.define_global_attrs(node_dataset, "Node", key)
                self.write_node_data(self.node_data[key], node_dataset)
                node_dataset.close()

    def write_node_data(self, data, dataset):
        """Write node level data from node data dictionary to NetCDF4 dataset.

        ## TODO
        - Figure out the number of time steps for 'nt' based on temporal range
        - Figure out best way to store nx by nt data

        Parameters
        ----------
        data: Pandas.DataFrame
            data frame that hold node-level SWOT data
        dataset: netCDF4.Dataset
            dataset to write node level data to
        """

        # Create dimension(s)
        dataset.createDimension("nt", None)
        dataset.createDimension("nx", data["node_id"].size)

        # Create coordinate variable(s)
        nt = dataset.createVariable("nt", "i4", ("nt",))
        nt.units = "TBD"
        nt.long_name = "time steps"
        nt[:] = range(0, 1)  ## TODO

        nx = dataset.createVariable("nx", "i4", ("nx",))
        nx.units = "node"
        nx.long_name = "number of nodes"
        nx[:] = range(1, data["node_id"].size + 1)

        # Create and write variable(s)
        self.write_node_vars(data, dataset)

    def write_node_vars(self, data, dataset):
        """Create and write node-level variables to NetCDF4 dataset.
        
        Parameters:
        data: Pandas.DataFrame
            data frame that hold node-level SWOT data
        dataset: netCDF4.Dataset
            node-level dataset to write variables to
        """
        reach_id = dataset.createVariable("reach_id", "i8", ("nx"))
        reach_id.long_name = "reach ID from prior river database"
        reach_id.comment = "Unique reach identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRT, where " \
            + "C=continent, B=basin, R=reach, T=type."
        reach_id[:] = data["reach_id"].astype(int)

        node_id = dataset.createVariable("node_id", "i8", ("nx"))
        node_id.long_name = "node ID of the node in the prior river database"
        node_id.comment = "Unique node identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRNNNT, " \
            + "where C=continent, B=basin, R=reach, N=node, T=type."
        node_id[:] = data["node_id"].astype(int)

        width = dataset.createVariable("width", "f8", ("nx"), 
            fill_value = self.FLOAT_FILL)
        width.long_name = "node width"
        width.units = "m"
        width.valid_min = 0.0
        width.valid_max = 100000
        width[:] = data["width"]

        wse = dataset.createVariable("wse", "f8", ("nx"), 
            fill_value = self.FLOAT_FILL)
        wse.long_name = "water surface elevation with respect to the geoid"
        wse.units = "m"
        wse.valid_min = -1000
        wse.valid_max = 100000
        wse[:] = data["wse"]
        
        node_q = dataset.createVariable("node_q", "i4", ("nx"), 
            fill_value=self.INT_FILL)
        node_q.long_name = "summary quality indicator for the node"
        node_q.standard_name = "status_flag"
        node_q.flag_masks = "TBD"
        node_q.flag_meanings = "good bad"
        node_q.flag_values = "0 1"
        node_q.valid_min = 0
        node_q.valid_max = 1
        node_q.comment = "Summary quality indicator for the node " \
            + "measurement. Values of 0 and 1 indicate nominal and " \
            + "off-nominal measurements."
        node_q[:] = data["node_q"]

        dark_frac = dataset.createVariable("dark_frac", "f8", ("nx"),
            fill_value=self.FLOAT_FILL)
        dark_frac.long_name = "fractional area of dark water"
        dark_frac.units = "1"
        dark_frac.valid_min = 0
        dark_frac.valid_max = 1
        dark_frac.comment = "Fraction of node area_total covered by dark water."
        dark_frac[:] = data["dark_frac"]

        ice_clim_f = dataset.createVariable("ice_clim_f", "i4", ("nx"),
            fill_value=self.INT_FILL)
        ice_clim_f.long_name = "climatological ice cover flag"
        ice_clim_f.standard_name = "status_flag"
        ice_clim_f.source = "Yang et al. (2020)"
        ice_clim_f.flag_meanings = "no_ice_cover uncertain_ice_cover full_ice_cover"
        ice_clim_f.flagalues = "0 1 2"
        ice_clim_f.valid_min = 0
        ice_clim_f.valid_max = 2
        ice_clim_f.comment = "Climatological ice cover flag indicating " \
            + "whether the node is ice-covered on the day of the " \
            + "observation based on external climatological information " \
            + "(not the SWOT measurement). Values of 0, 1, and 2 indicate " \
            + "that the node is likely not ice covered, may or may not be " \
            + "partially or fully ice covered, and likely fully ice covered, " \
            + "respectively."
        ice_clim_f[:] = data["ice_clim_f"]

        ice_dyn_f = dataset.createVariable("ice_dyn_f", "i4", ("nx"),
            fill_value=self.INT_FILL)
        ice_dyn_f.long_name = "dynamical ice cover flag"
        ice_dyn_f.standard_name = "status_flag"
        ice_dyn_f.source = "Yang et al. (2020)"
        ice_dyn_f.flag_meanings = "no_ice_cover uncertain_ice_cover full_ice_cover"
        ice_dyn_f.flagalues = "0 1 2"
        ice_dyn_f.valid_min = 0
        ice_dyn_f.valid_max = 2
        ice_dyn_f.comment = "Dynamic ice cover flag indicating whether " \
            + "the surface is ice-covered on the day of the observation " \
            + "based on analysis of external satellite optical data. Values " \
            + "of 0, 1, and 2 indicate that the node is not ice covered, " \
            + "partially ice covered, and fully ice covered, respectively."
        ice_dyn_f[:] = data["ice_dyn_f"] 

        partial_f = dataset.createVariable("partial_f", "i4", ("nx"),
            fill_value=self.INT_FILL)
        partial_f.long_name = "partial reach coverage flag"
        partial_f.standard_name = "status_flag"
        partial_f.flag_meanings = "covered not_covered"
        partial_f.flagalues = "0 1"
        partial_f.valid_min = 0
        partial_f.valid_max = 2
        partial_f.comment = "Flag that indicates only partial node " \
            + "coverage. The flag is 0 if at least 10 pixels have a valid " \
            + "WSE measurement; the flag is 1 otherwise and node-level " \
            + "quantities are not computed."
        partial_f[:] = data["partial_f"]

        n_good_pix = dataset.createVariable("n_good_pix", "i4", ("nx"),
            fill_value = self.INT_FILL)
        n_good_pix.long_name = "number of pixels that have a valid WSE"
        n_good_pix.units = "1"
        n_good_pix.valid_min = 0
        n_good_pix.valid_max = 100000
        n_good_pix.comment = "Number of pixels assigned to the node that " \
            + "have a valid node WSE."
        n_good_pix[:] = data["n_good_pix"]

        xovr_cal_q = dataset.createVariable("xovr_cal_q", "i4", ("nx"),
            fill_value=self.INT_FILL)
        xovr_cal_q.long_name = "quality of the cross-over calibration"
        xovr_cal_q.flag_masks = "TBD"
        xovr_cal_q.flag_meanings = "TBD"
        xovr_cal_q.flagalues = "T B D"
        xovr_cal_q.valid_min = 0
        xovr_cal_q.valid_max = 1
        xovr_cal_q.comment = "Quality of the cross-over calibration."
        xovr_cal_q[:] = data["xovr_cal_q"]

    def write_reach_data(self, data, dataset):
        """Write reach level data from reach data dictionary to NetCDF4 dataset.

        ## TODO
        - Figure out the number of time steps for 'nt' based on temporal range
        - Figure out the best way to store nt and nr for reach level

        Parameters
        ----------
        data: Pandas.DataFrame
            data frame that hold reach-level SWOT data
        dataset: netCDF4.Dataset
            dataset to write reach level data to
        """

        # Create dimension(s)
        dataset.createDimension("nr", data["reach_id"].size)
        dataset.createDimension("nt", None)
        
        # Create coordinate variable(s)
        nr = dataset.createVariable("nr", "i4", ("nr",))
        nr.units = "reach"
        nr.long_name = "number of reaches"
        nr[:] = range(1, data["reach_id"].size + 1)

        nt = dataset.createVariable("nt", "i4", ("nt",))
        nt.units = "TBD"
        nt.long_name = "time steps"
        nt[:] = range(0, 1)  ## TODO

        # Create and write variable(s)
        self.write_reach_vars(data, dataset)        

    def write_reach_vars(self, data, dataset):
        """Create and write reach-level variables to NetCDF4 dataset.
        
        Parameters:
        data: Pandas.DataFrame
            data frame that hold reach-level SWOT data
        dataset: netCDF4.Dataset
            reach-level dataset to write variables to
        """

        reach_id = dataset.createVariable("reach_id", "i8", ("nr"))
        reach_id.long_name = "reach ID from prior river database"
        reach_id.comment = "Unique reach identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRT, where " \
            + "C=continent, B=basin, R=reach, T=type."
        reach_id[:] = data["reach_id"].astype(int)

        dxa = dataset.createVariable("d_x_area", "f8", ("nr"),
            fill_value=self.FLOAT_FILL)
        dxa.long_name = "change in cross-sectional area"
        dxa.units = "m^2"
        dxa.valid_min = -10000000
        dxa.valid_max = 10000000
        dxa[:] = data["d_x_area"]

        slope2 = dataset.createVariable("slope2", "f8", ("nr"),
            fill_value=self.FLOAT_FILL)
        slope2.long_name = "enhanced water surface slope with respect to geoid"
        slope2.units = "m/m"
        slope2.valid_min = -0.001
        slope2.valid_max = 0.1
        slope2[:] = data["slope2"]

        width = dataset.createVariable("width", "f8", ("nr"), 
            fill_value=self.FLOAT_FILL)
        width.long_name = "reach width"
        width.units = "m"
        width.valid_min = 0.0
        width.valid_max = 100000
        width[:] = data["width"]

        wse = dataset.createVariable("wse", "f8", ("nr"),
            fill_value=self.FLOAT_FILL)
        wse.long_name = "water surface elevation with respect to the geoid"
        wse.units = "m"
        wse.valid_min = -1000
        wse.valid_max = 100000
        wse[:] = data["wse"]

        reach_q = dataset.createVariable("reach_q", "i4", ("nr"),
            fill_value=self.INT_FILL)
        reach_q.long_name = "summary quality indicator for the reach"
        reach_q.standard_name = "summary quality indicator for the reach"
        reach_q.flag_masks = "TBD"
        reach_q.flag_meanings = "good bad"
        reach_q.flagalues = "0 1"
        reach_q.valid_min = 0
        reach_q.valid_max = 1
        reach_q.comment = "Summary quality indicator for the reach " \
            + "measurement. Values of 0 and 1 indicate nominal (good) " \
            + "and off-nominal (suspect) measurements."
        reach_q[:] = data["reach_q"]

        dark_frac = dataset.createVariable("dark_frac", "f8", ("nr"),
            fill_value=self.FLOAT_FILL)
        dark_frac.long_name = "fractional area of dark water"
        dark_frac.units = "1"
        dark_frac.valid_min = -1000
        dark_frac.valid_max = 10000
        dark_frac.comment = "Fraction of reach area_total covered by dark water."
        dark_frac[:] = data["dark_frac"]

        ice_clim_f = dataset.createVariable("ice_clim_f", "i4", ("nr"),
            fill_value=self.INT_FILL)
        ice_clim_f.long_name = "climatological ice cover flag"
        ice_clim_f.standard_name = "status_flag"
        ice_clim_f.source = "Yang et al. (2020)"
        ice_clim_f.flag_meanings = "no_ice_cover uncertain_ice_cover full_ice_cover"
        ice_clim_f.flagalues = "0 1 2"
        ice_clim_f.valid_min = 0
        ice_clim_f.valid_max = 2
        ice_clim_f.comment = "Climatological ice cover flag indicating " \
            + "whether the reach is ice-covered on the day of the " \
            + "observation based on external climatological information " \
            + "(not the SWOT measurement). Values of 0, 1, and 2 indicate " \
            + "that the reach is likely not ice covered, may or may not be " \
            + "partially or fully ice covered, and likely fully ice covered, " \
            + "respectively."
        ice_clim_f[:] = data["ice_clim_f"]

        ice_dyn_f = dataset.createVariable("ice_dyn_f", "i4", ("nr"),
            fill_value=self.INT_FILL)
        ice_dyn_f.long_name = "dynamical ice cover flag"
        ice_dyn_f.standard_name = "status_flag"
        ice_dyn_f.source = "Yang et al. (2020)"
        ice_dyn_f.flag_meanings = "no_ice_cover uncertain_ice_cover full_ice_cover"
        ice_dyn_f.flagalues = "0 1 2"
        ice_dyn_f.valid_min = 0
        ice_dyn_f.valid_max = 2
        ice_dyn_f.comment = "Dynamic ice cover flag indicating whether " \
            + "the surface is ice-covered on the day of the observation " \
            + "based on analysis of external satellite optical data. Values " \
            + "of 0, 1, and 2 indicate that the reach is not ice covered, " \
            + "partially ice covered, and fully ice covered, respectively."
        ice_dyn_f[:] = data["ice_dyn_f"] 

        partial_f = dataset.createVariable("partial_f", "i4", ("nr"),
            fill_value=self.INT_FILL)
        partial_f.long_name = "partial reach coverage flag"
        partial_f.standard_name = "status_flag"
        partial_f.flag_meanings = "covered not_covered"
        partial_f.flagalues = "0 1"
        partial_f.valid_min = 0
        partial_f.valid_max = 2
        partial_f.comment = "Flag that indicates only partial reach " \
            + "coverage. The flag is 0 if at least half the nodes of the " \
            + "reach have valid WSE measurements; the flag is 1 otherwise " \
            + "and reach-level quantities are not computed."
        partial_f[:] = data["partial_f"]

        n_good_nod = dataset.createVariable("n_good_nod", "i4", ("nr"),
            fill_value=self.INT_FILL)
        n_good_nod.long_name = "number of nodes in the reach that have a " \
            + "valid WSE"
        n_good_nod.units = "1"
        n_good_nod.valid_min = 0
        n_good_nod.valid_max = 100
        n_good_nod.comment = "Number of nodes in the reach that have " \
            + "a valid node WSE. Note that the total number of nodes " \
            + "from the prior river database is given by p_n_nodes."
        n_good_nod[:] = data["n_good_nod"]

        obs_frac_n = dataset.createVariable("obs_frac_n", "f8", ("nr"),
            fill_value=self.FLOAT_FILL)
        obs_frac_n.long_name = "fraction of nodes that have a valid WSE"
        obs_frac_n.units = "1"
        obs_frac_n.valid_min = 0
        obs_frac_n.valid_max = 1
        obs_frac_n.comment = "Fraction of nodes (n_good_nod/p_n_nodes) " \
            + "in the reach that have a valid node WSE. The value is " \
            + "between 0 and 1."
        obs_frac_n[:] = data["obs_frac_n"]

        xovr_cal_q = dataset.createVariable("xovr_cal_q", "i4", ("nr"),
            fill_value=self.INT_FILL)
        xovr_cal_q.long_name = "quality of the cross-over calibration"
        xovr_cal_q.flag_masks = "TBD"
        xovr_cal_q.flag_meanings = "TBD"
        xovr_cal_q.flagalues = "T B D"
        xovr_cal_q.valid_min = 0
        xovr_cal_q.valid_max = 1
        xovr_cal_q.comment = "Quality of the cross-over calibration."
        xovr_cal_q[:] = data["xovr_cal_q"]