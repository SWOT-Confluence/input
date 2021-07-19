# Standard imports
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

# Third-party imports
from netCDF4 import Dataset
import numpy as np

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
    temp_dir: tempfile.TemporaryDirectory
        temporary directory to write NetCDF files to prior to upload

    Methods
    -------
    __create_dimensions( nx, nt, dataset)
        Create dimensions and coordinate variables for dataset.
    __define_global_attrs(dataset, level, cont)
        Set global attributes for NetCDF dataset file
    write_data()
        writes SWOT data dictionaries to NetCDF files organized by continent
    __write_data(nc_file)
        writes node level data to NetCDF file in node group
    __write_reach_data(nc_file)
        writes reach level data to NetCDF file in reach group
    """

    FLOAT_FILL = -999999999999
    INT_FILL = -999

    def __init__(self, node_data, reach_data):
        """
        Parameters
        ----------
        node_data: dict
            dictionary with continent keys and dataframe value of SWOT node data
        reach_data: dict
            dictionary with continent keys and dataframe value of SWOT reach data
        """
        self.continents = { "af": "Africa", "eu": "Europe and Middle East",
            "si": "Siberia", "as": "Central and Southeast Asia",
            "au": "Australia and Oceania", "sa": "South America",
            "na": "North America and Caribbean", "ar": "North American Arctic",
            "gr": "Greenland" }
        self.temp_dir = TemporaryDirectory()
        self.node_data = node_data
        self.reach_data = reach_data

    def __create_dimensions(self, nx, nt, dataset):
        """Create dimensions and coordinate variables for dataset.
        
        Parameters
        ----------
        nx: int
            number of nodes
        nt: int
            number of time steps
        dataset: netCDF4.Dataset
            dataset to write node level data to
        """

         # Create dimension(s)
        dataset.createDimension("nt", nt)
        dataset.createDimension("nx", nx)

        # Create coordinate variable(s)
        nt_v = dataset.createVariable("nt", "i4", ("nt",))
        nt_v.units = "day"
        nt_v.long_name = "time steps"
        nt_v[:] = range(0, nt)

        nx_v = dataset.createVariable("nx", "i4", ("nx",))
        nx_v.units = "node"
        nx_v.long_name = "number of nodes"
        nx_v[:] = range(1, nx + 1)

    def __define_global_attrs(self, dataset, reach_id, cont):
        """Set global attributes for NetCDF dataset file.

        Currently sets title, history, and continent.

        Parameter
        ---------
        dataset: netCDF4.Dataset
            netCDF4 dataset to set global attirbutes for
        reach_id: int
           unique reach identifier
        cont: str
            continent that data was obtained for
        
        ## TODO:
        - cycle number, pass number, time_coverage (range)
        """

        dataset.title = f"SWOT Data for Reach {reach_id}"
        dataset.reach_id = reach_id
        dataset.history = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
        dataset.continent = self.continents[cont]

    def write_data(self):
        """Writes node and reach level SWOT data to NetCDF format.
        
        Files are placed in a temporary directory.

        ## TODO
        - remove if statement testing for None as all keys should be populated
        """

        for key in self.continents.keys():
            if self.reach_data[key] is not None and self.node_data[key] is not None:
                reach_ids = self.reach_data[key]["reach_id"]
                for reach_id in reach_ids:
                    # NetCDF4 dataset
                    reach_file = Path(self.temp_dir.name) / f"{reach_id}_SWOT.nc"
                    dataset = Dataset(reach_file, 'w', format="NETCDF4")
                    self.__define_global_attrs(dataset, reach_id, key)
                    reach_group = dataset.createGroup("reach")
                    node_group = dataset.createGroup("node")

                    # Dimension and data
                    reach_i = np.where(self.reach_data[key]["reach_id"] == reach_id)
                    node_i = np.where(self.node_data["na"]["reach_id"] == reach_id)
                    self.__create_dimensions(np.size(node_i), 
                                            self.node_data[key]["nt"], dataset)

                    # Reach and node data
                    self.__write_reach_vars(self.reach_data[key], reach_group, reach_id, reach_i)
                    self.__write_node_vars(self.node_data[key], node_group, reach_id, node_i)

                    dataset.close()
        
    def __write_node_vars(self, data, dataset, reach_id, indexes):
        """Create and write reach-level variables to NetCDF4 dataset.
        
        Parameters:
        data: Pandas.DataFrame
            data frame that hold reach-level SWOT data
        dataset: netCDF4.Dataset
            reach-level dataset to write variables to
        reach_id: int
            unique reach identifier value
        indexes: list
            node_id int indexes to locate values in data
        """

        reach_id_v = dataset.createVariable("reach_id", "i8")
        reach_id_v.long_name = "reach ID from prior river database"
        reach_id_v.comment = "Unique reach identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRT, where " \
            + "C=continent, B=basin, R=reach, T=type."
        reach_id_v.assignValue(reach_id.item())

        node_id = dataset.createVariable("node_id", "i8", ("nx",))
        node_id.long_name = "node ID of the node in the prior river database"
        node_id.comment = "Unique node identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRNNNT, " \
            + "where C=continent, B=basin, R=reach, N=node, T=type."
        node_id[:] = data["node_id"][indexes].astype(int)

        dxa = dataset.createVariable("d_x_area", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        dxa.long_name = "change in cross-sectional area"
        dxa.units = "m^2"
        dxa.valid_min = -10000000
        dxa.valid_max = 10000000
        dx = np.transpose(np.squeeze(data["d_x_area"][:, indexes]))
        np.nan_to_num(dx, copy=False, nan=self.FLOAT_FILL)
        dxa[:] = dx

        slope2 = dataset.createVariable("slope2", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        slope2.long_name = "enhanced water surface slope with respect to geoid"
        slope2.units = "m/m"
        slope2.valid_min = -0.001
        slope2.valid_max = 0.1
        slope2.comment = "slope2 extracted from reach level data and " \
            + "appended to node."
        s = np.transpose(np.squeeze(data["slope2"][:, indexes]))
        np.nan_to_num(s, copy=False, nan=self.FLOAT_FILL)
        slope2[:] = s

        width = dataset.createVariable("width", "f8", ("nx", "nt"), 
            fill_value = self.FLOAT_FILL)
        width.long_name = "node width"
        width.units = "m"
        width.valid_min = 0.0
        width.valid_max = 100000
        w = np.transpose(np.squeeze(data["width"][:, indexes]))
        np.nan_to_num(w, copy=False, nan=self.FLOAT_FILL)
        width[:] = w

        wse = dataset.createVariable("wse", "f8", ("nx", "nt"), 
            fill_value = self.FLOAT_FILL)
        wse.long_name = "water surface elevation with respect to the geoid"
        wse.units = "m"
        wse.valid_min = -1000
        wse.valid_max = 100000
        ws = np.transpose(np.squeeze(data["wse"][:, indexes]))
        np.nan_to_num(ws, copy=False, nan=self.FLOAT_FILL)
        wse[:] = ws
        
        node_q = dataset.createVariable("node_q", "i4", ("nx", "nt"), 
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
        nq = np.transpose(np.squeeze(data["node_q"][:, indexes]))
        np.nan_to_num(nq, copy=False, nan=self.INT_FILL)
        node_q[:] = nq

        dark_frac = dataset.createVariable("dark_frac", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        dark_frac.long_name = "fractional area of dark water"
        dark_frac.units = "1"
        dark_frac.valid_min = 0
        dark_frac.valid_max = 1
        dark_frac.comment = "Fraction of node area_total covered by dark water."
        df = np.transpose(np.squeeze(data["dark_frac"][:, indexes]))
        np.nan_to_num(df, copy=False, nan=self.FLOAT_FILL)
        dark_frac[:] = df

        ice_clim_f = dataset.createVariable("ice_clim_f", "i4", ("nx", "nt"),
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
        icf = np.transpose(np.squeeze(data["ice_clim_f"][:, indexes]))
        np.nan_to_num(icf, copy=False, nan=self.INT_FILL)
        ice_clim_f[:] = icf

        ice_dyn_f = dataset.createVariable("ice_dyn_f", "i4", ("nx", "nt"),
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
        idf = np.transpose(np.squeeze(data["ice_dyn_f"][:, indexes]))
        np.nan_to_num(idf, copy=False, nan=self.INT_FILL)
        ice_dyn_f[:] = idf

        partial_f = dataset.createVariable("partial_f", "i4", ("nx", "nt"),
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
        pf = np.transpose(np.squeeze(data["partial_f"][:, indexes]))
        np.nan_to_num(pf, copy=False, nan=self.INT_FILL)
        partial_f[:] = pf

        n_good_pix = dataset.createVariable("n_good_pix", "i4", ("nx", "nt"),
            fill_value = self.INT_FILL)
        n_good_pix.long_name = "number of pixels that have a valid WSE"
        n_good_pix.units = "1"
        n_good_pix.valid_min = 0
        n_good_pix.valid_max = 100000
        n_good_pix.comment = "Number of pixels assigned to the node that " \
            + "have a valid node WSE."
        ngp = np.transpose(np.squeeze(data["n_good_pix"][:, indexes]))
        np.nan_to_num(ngp, copy=False, nan=self.INT_FILL)
        n_good_pix[:] = ngp

        xovr_cal_q = dataset.createVariable("xovr_cal_q", "i4", ("nx", "nt"),
            fill_value=self.INT_FILL)
        xovr_cal_q.long_name = "quality of the cross-over calibration"
        xovr_cal_q.flag_masks = "TBD"
        xovr_cal_q.flag_meanings = "TBD"
        xovr_cal_q.flagalues = "T B D"
        xovr_cal_q.valid_min = 0
        xovr_cal_q.valid_max = 1
        xovr_cal_q.comment = "Quality of the cross-over calibration."
        xcq = np.transpose(np.squeeze(data["xovr_cal_q"][:, indexes]))
        np.nan_to_num(xcq, copy=False, nan=self.INT_FILL)
        xovr_cal_q[:] = xcq  

    def __write_reach_vars(self, data, dataset, reach_id, index):
        """Create and write reach-level variables to NetCDF4 dataset.
        
        Parameters:
        data: Pandas.DataFrame
            data frame that hold reach-level SWOT data
        dataset: netCDF4.Dataset
            reach-level dataset to write variables to
        reach_id: int
            unique reach identifier value
        index: int
            reach_id index to locate values in data
        """

        reach_id_v = dataset.createVariable("reach_id", "i8")
        reach_id_v.long_name = "reach ID from prior river database"
        reach_id_v.comment = "Unique reach identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRT, where " \
            + "C=continent, B=basin, R=reach, T=type."
        reach_id_v.assignValue(reach_id.item())
        
        dxa = dataset.createVariable("d_x_area", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        dxa.long_name = "change in cross-sectional area"
        dxa.units = "m^2"
        dxa.valid_min = -10000000
        dxa.valid_max = 10000000
        d = np.squeeze(data["d_x_area"][:, index].flatten('C'))
        np.nan_to_num(d, copy=False, nan=self.FLOAT_FILL)
        dxa[:] = d

        slope2 = dataset.createVariable("slope2", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        slope2.long_name = "enhanced water surface slope with respect to geoid"
        slope2.units = "m/m"
        slope2.valid_min = -0.001
        slope2.valid_max = 0.1
        s = np.squeeze(data["slope2"][:, index].flatten('C'))
        np.nan_to_num(s, copy=False, nan=self.FLOAT_FILL)
        slope2[:] = s

        width = dataset.createVariable("width", "f8", ("nt",), 
            fill_value=self.FLOAT_FILL)
        width.long_name = "reach width"
        width.units = "m"
        width.valid_min = 0.0
        width.valid_max = 100000
        w = np.squeeze(data["width"][:, index].flatten('C'))
        np.nan_to_num(w, copy=False, nan=self.FLOAT_FILL)
        width[:] = w

        wse = dataset.createVariable("wse", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        wse.long_name = "water surface elevation with respect to the geoid"
        wse.units = "m"
        wse.valid_min = -1000
        wse.valid_max = 100000
        ws = np.squeeze(data["wse"][:, index].flatten('C'))
        np.nan_to_num(ws, copy=False, nan=self.FLOAT_FILL)
        wse[:] = ws

        reach_q = dataset.createVariable("reach_q", "i4", ("nt",),
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
        rq = np.squeeze(data["reach_q"][:, index].flatten('C'))
        np.nan_to_num(rq, copy=False, nan=self.INT_FILL)
        reach_q[:] = rq

        dark_frac = dataset.createVariable("dark_frac", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        dark_frac.long_name = "fractional area of dark water"
        dark_frac.units = "1"
        dark_frac.valid_min = -1000
        dark_frac.valid_max = 10000
        dark_frac.comment = "Fraction of reach area_total covered by dark water."
        df = np.squeeze(data["dark_frac"][:, index].flatten('C'))
        np.nan_to_num(df, copy=False, nan=self.FLOAT_FILL)
        dark_frac[:] = df

        ice_clim_f = dataset.createVariable("ice_clim_f", "i4", ("nt",),
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
        icf = np.squeeze(data["ice_clim_f"][:, index].flatten('C'))
        np.nan_to_num(icf, copy=False, nan=self.INT_FILL)
        ice_clim_f[:] = icf

        ice_dyn_f = dataset.createVariable("ice_dyn_f", "i4", ("nt",),
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
        idf = np.squeeze(data["ice_dyn_f"][:, index].flatten('C'))
        np.nan_to_num(idf, copy=False, nan=self.INT_FILL)
        ice_dyn_f[:] = idf

        partial_f = dataset.createVariable("partial_f", "i4", ("nt",),
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
        pf = np.squeeze(data["partial_f"][:, index].flatten('C'))
        np.nan_to_num(pf, copy=False, nan=self.INT_FILL)
        partial_f[:] = pf

        n_good_nod = dataset.createVariable("n_good_nod", "i4", ("nt",),
            fill_value=self.INT_FILL)
        n_good_nod.long_name = "number of nodes in the reach that have a " \
            + "valid WSE"
        n_good_nod.units = "1"
        n_good_nod.valid_min = 0
        n_good_nod.valid_max = 100
        n_good_nod.comment = "Number of nodes in the reach that have " \
            + "a valid node WSE. Note that the total number of nodes " \
            + "from the prior river database is given by p_n_nodes."
        ngn = np.squeeze(data["n_good_nod"][:, index].flatten('C'))
        np.nan_to_num(ngn, copy=False, nan=self.INT_FILL)
        n_good_nod[:] = ngn

        obs_frac_n = dataset.createVariable("obs_frac_n", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        obs_frac_n.long_name = "fraction of nodes that have a valid WSE"
        obs_frac_n.units = "1"
        obs_frac_n.valid_min = 0
        obs_frac_n.valid_max = 1
        obs_frac_n.comment = "Fraction of nodes (n_good_nod/p_n_nodes) " \
            + "in the reach that have a valid node WSE. The value is " \
            + "between 0 and 1."
        ofn = np.squeeze(data["obs_frac_n"][:, index].flatten('C'))
        np.nan_to_num(ofn, copy=False, nan=self.FLOAT_FILL)
        obs_frac_n[:] = ofn

        xovr_cal_q = dataset.createVariable("xovr_cal_q", "i4", ("nt",),
            fill_value=self.INT_FILL)
        xovr_cal_q.long_name = "quality of the cross-over calibration"
        xovr_cal_q.flag_masks = "TBD"
        xovr_cal_q.flag_meanings = "TBD"
        xovr_cal_q.flagalues = "T B D"
        xovr_cal_q.valid_min = 0
        xovr_cal_q.valid_max = 1
        xovr_cal_q.comment = "Quality of the cross-over calibration."
        xcq = np.squeeze(data["xovr_cal_q"][:, index].flatten('C'))
        np.nan_to_num(xcq, copy=False, nan=self.INT_FILL)
        xovr_cal_q[:] = xcq