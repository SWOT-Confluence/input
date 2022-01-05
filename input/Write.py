# Standard imports
from datetime import datetime

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np

class Write:
    """A class that takes SWOT and SoS data and writes intermediate input data.
    
    Intermediate data is stored in NetCDF file format and is taken from various
    AWS S3 buckets.

    Attributes
    ----------
    CONTINENTS: dict
        dictionary of continent IDs (keys) and continent names (values)
    FLOAT_FILL: float
        value to use when missing or invalid data is encountered for float
    INT_FILL: int
        value to use when missing or invalid data is encountered for integers
    node_data: dict
        dictionary with continent keys and dataframe value of SWOT node data
    obs_times: list
            list of string cycle/pass identifiers
    output_dir: Path
        path to output directory on EFS 'input' mount
    reach_data: dict
        dictionary with continent keys and dataframe value of SWOT reach data

    Methods
    -------
    copy_sos_data(confluence_fs)
        copy S3 SoS data to output directory
    __create_dimensions( nx, nt, dataset)
        create dimensions and coordinate variables for dataset
    __define_global_attrs(dataset, level, cont)
        Set global attributes for NetCDF dataset file
    write_data()
        writes SWOT data dictionaries to NetCDF files organized by continent
    __write_node_data(nc_file)
        writes node level data to NetCDF file in node group
    __write_reach_data(nc_file)
        writes reach level data to NetCDF file in reach group
    """

    CONTINENTS = { 1: "AF", 2: "EU", 3: "AS", 4: "AS", 5: "OC", 6: "SA", 7: "NA", 8: "NA", 9:"NA" }
    FLOAT_FILL = -999999999999
    INT_FILL = -999

    def __init__(self, node_data, reach_data, obs_times, output_dir):
        """
        Parameters
        ----------
        node_data: dict
            dictionary with continent keys and dataframe value of SWOT node data
        reach_data: dict
            dictionary with continent keys and dataframe value of SWOT reach data
        obs_times: list
            list of string cycle/pass identifiers
        output_dir: Path
            path to output directory on EFS 'input' mount
        """
        
        self.node_data = node_data
        self.obs_times = obs_times
        self.output_dir = output_dir
        self.reach_data = reach_data

    def __create_dimensions(self, dataset, nx):
        """Create dimensions and coordinate variables for dataset.
        
        Parameters
        ----------
        dataset: netCDF4.Dataset
            dataset to write node level data to
        nx: int
         integer number of nodes
        """

         # Create dimension(s)
        dataset.createDimension("nt", len(self.obs_times))
        dataset.createDimension("nx", nx)

        # Create coordinate variable(s)
        nt_v = dataset.createVariable("nt", "i4", ("nt",))
        nt_v.units = "pass"
        nt_v.long_name = "time steps"
        nt_v[:] = range(len(self.obs_times))

        nx_v = dataset.createVariable("nx", "i4", ("nx",))
        nx_v.units = "node"
        nx_v.long_name = "number of nodes"
        nx_v[:] = range(1, nx + 1)

    def __define_global_attrs(self, dataset, reach_id):
        """Set global attributes for NetCDF dataset file.

        Currently sets title, history, and continent.

        Parameter
        ---------
        dataset: netCDF4.Dataset
            netCDF4 dataset to set global attirbutes for
        reach_id: int
           unique reach identifier
        """

        dataset.title = f"SWOT Data for Reach {reach_id}"
        dataset.reach_id = int(reach_id)
        dataset.history = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
        dataset.continent = self.CONTINENTS[int(str(reach_id)[0])]

    def write_data(self, reach_id, node_ids):
        """Writes node and reach level SWOT data to NetCDF format.
        
        TODO:
        - Figure out maximum cycle/pass length.
        
        Parameters
        ----------
        reach_id: str
            string reach identifier
        node_ids: list
            list of string node identifiers
        """

        # NetCDF4 dataset
        reach_file = self.output_dir / "swot" / f"{reach_id}_SWOT.nc"
        dataset = Dataset(reach_file, 'w', format="NETCDF4")
        self.__define_global_attrs(dataset, reach_id)

        # Dimension and data
        self.__create_dimensions(dataset, len(node_ids))
        
        # Global observation variable
        dataset.createDimension('nchars', 10)
        obs = dataset.createVariable("observations", "S1", ("nt", "nchars"))
        obs.units = "pass"
        obs.long_name = "cycle/pass observations"
        obs.comment = "A list of cycle and pass numeric identifiers that " \
            + "identify each reach and node observation. An array element " \
            + "is comprised of 'cycle/pass' as a string value."
        obs[:] = stringtochar(np.array(self.obs_times, dtype="S10"))

        # Reach and node data
        reach_group = dataset.createGroup("reach")
        self.__write_reach_vars(reach_group, reach_id)
        node_group = dataset.createGroup("node")
        self.__write_node_vars(node_group, reach_id, node_ids)

        dataset.close()
        
    def __write_node_vars(self, dataset, reach_id, node_ids):
        """Create and write reach-level variables to NetCDF4 dataset.

        TODO:
        - d_x_area_u max value is larger than PDD max value documentation
        
        Parameters:
        dataset: netCDF4.Dataset
            reach-level dataset to write variables to
        reach_id: str
            unique reach identifier value
        node_ids: list
            list of string node identifiers
        """

        reach_id_v = dataset.createVariable("reach_id", "i8")
        reach_id_v.long_name = "reach ID from prior river database"
        reach_id_v.comment = "Unique reach identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRT, where " \
            + "C=continent, B=basin, R=reach, T=type."
        reach_id_v.assignValue(int(reach_id))

        node_id = dataset.createVariable("node_id", "i8", ("nx",))
        node_id.long_name = "node ID of the node in the prior river database"
        node_id.comment = "Unique node identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRNNNT, " \
            + "where C=continent, B=basin, R=reach, N=node, T=type."
        node_id[:] = np.array(node_ids, dtype=np.int64)
        
        time = dataset.createVariable("time", "f8", ("nx", "nt"))
        time.long_name = "time (UTC)"
        time.calendar = "gregorian"
        time.tai_utc_difference = "[value of TAI-UTC at time of first record]"
        time.leap_second = "YYYY-MM-DD hh:mm:ss"
        time.units = "seconds since 2000-01-01 00:00:00.000"
        time.comment = "Time of measurement in seconds in the UTC time " \
            + "scale since 1 Jan 2000 00:00:00 UTC. [tai_utc_difference] is " \
            + "the difference between TAI and UTC reference time (seconds) " \
            + "for the first measurement of the data set. If a leap second " \
            + "occurs within the data set, the metadata leap_second is set " \
            + "to the UTC time at which the leap second occurs."
        time[:] = self.node_data["time"]

        dxa = dataset.createVariable("d_x_area", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        dxa.long_name = "change in cross-sectional area"
        dxa.units = "m^2"
        dxa.valid_min = -10000000
        dxa.valid_max = 10000000
        dxa.comment = "Change in channel cross sectional area from the value reported in the prior river database. Extracted from reach-level and appended to node."
        dxa[:] = np.nan_to_num(self.node_data["d_x_area"], copy=True, nan=self.FLOAT_FILL)

        dxa_u = dataset.createVariable("d_x_area_u", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        dxa_u.long_name = "total uncertainty of the change in the cross-sectional area"
        dxa_u.units = "m^2"
        dxa_u.valid_min = 0
        dxa_u.valid_max = 1000000000    # TODO fix to match PDD
        dxa_u.comment = "Total one-sigma uncertainty (random and systematic) in the change in the cross-sectional area. Extracted from reach-level and appended to node."
        dxa_u[:] = np.nan_to_num(self.node_data["d_x_area_u"], copy=True, nan=self.FLOAT_FILL)

        slope2 = dataset.createVariable("slope2", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        slope2.long_name = "enhanced water surface slope with respect to geoid"
        slope2.units = "m/m"
        slope2.valid_min = -0.001
        slope2.valid_max = 0.1
        slope2.comment = "Enhanced water surface slope relative to the geoid, produced using a smoothing of the node wse. The upstream or downstream direction is defined by the prior river database. A positive slope means that the downstream WSE is lower. Extracted from reach-level and appended to node."
        slope2[:] = np.nan_to_num(self.node_data["slope2"], copy=True, nan=self.FLOAT_FILL)

        slope2_u = dataset.createVariable("slope2_u", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        slope2_u.long_name = "uncertainty in the enhanced water surface slope"
        slope2_u.units = "m/m"
        slope2_u.valid_min = 0
        slope2_u.valid_max = 0.1
        slope2_u.comment = "Total one-sigma uncertainty (random and systematic) in the enhanced water surface slope, including uncertainties of corrections and variation about the fit. Extracted from reach-level and appended to node."
        slope2_u[:] = np.nan_to_num(self.node_data["slope2_u"], copy=True, nan=self.FLOAT_FILL)

        width = dataset.createVariable("width", "f8", ("nx", "nt"), 
            fill_value = self.FLOAT_FILL)
        width.long_name = "node width"
        width.units = "m"
        width.valid_min = 0.0
        width.valid_max = 100000
        width.comment = "Node width."
        width[:] = np.nan_to_num(self.node_data["width"], copy=True, nan=self.FLOAT_FILL)

        width_u = dataset.createVariable("width_u", "f8", ("nx", "nt"), 
            fill_value = self.FLOAT_FILL)
        width_u.long_name = "total uncertainty in the node width"
        width_u.units = "m"
        width_u.valid_min = 0
        width_u.valid_max = 10000
        width_u.comment = "Total one-sigma uncertainty (random and systematic) in the node width."
        width_u[:] = np.nan_to_num(self.node_data["width_u"], copy=True, nan=self.FLOAT_FILL)

        wse = dataset.createVariable("wse", "f8", ("nx", "nt"), fill_value = self.FLOAT_FILL)
        wse.long_name = "water surface elevation with respect to the geoid"
        wse.units = "m"
        wse.valid_min = -1000
        wse.valid_max = 100000
        wse.comment = "Fitted node water surface elevation, relative to the provided model of the geoid (geoid_hght), with all corrections for media delays (wet and dry troposphere, and ionosphere), crossover correction, and tidal effects (solid_tide, load_tidef, and pole_tide) applied."
        wse[:] = np.nan_to_num(self.node_data["wse"], copy=True, nan=self.FLOAT_FILL)
        
        wse_u = dataset.createVariable("wse_u", "f8", ("nx", "nt"), fill_value = self.FLOAT_FILL)
        wse_u.long_name = "total uncertainty in the water surface elevation"
        wse_u.units = "m"
        wse_u.valid_min = 0.0
        wse_u.valid_max = 999999
        wse_u.comment = "Total one-sigma uncertainty (random and systematic) in the node WSE, including uncertainties of corrections, and variation about the fit."
        wse_u[:] = np.nan_to_num(self.node_data["wse_u"], copy=True, nan=self.FLOAT_FILL)

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
        node_q[:] = np.nan_to_num(self.node_data["node_q"], copy=True, nan=self.INT_FILL)

        dark_frac = dataset.createVariable("dark_frac", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        dark_frac.long_name = "fractional area of dark water"
        dark_frac.units = "1"
        dark_frac.valid_min = 0
        dark_frac.valid_max = 1
        dark_frac.comment = "Fraction of node area_total covered by dark water."
        dark_frac[:] = np.nan_to_num(self.node_data["dark_frac"], copy=True, nan=self.FLOAT_FILL)

        ice_clim_f = dataset.createVariable("ice_clim_f", "i4", ("nx", "nt"),
            fill_value=self.INT_FILL)
        ice_clim_f.long_name = "climatological ice cover flag"
        ice_clim_f.standard_name = "status_flag"
        ice_clim_f.source = "Yang et al. (2020)"
        ice_clim_f.flag_meanings = "no_ice_cover uncertain_ice_cover full_ice_cover"
        ice_clim_f.flag_values = "0 1 2"
        ice_clim_f.valid_min = 0
        ice_clim_f.valid_max = 2
        ice_clim_f.comment = "Climatological ice cover flag indicating " \
            + "whether the node is ice-covered on the day of the " \
            + "observation based on external climatological information " \
            + "(not the SWOT measurement). Values of 0, 1, and 2 indicate " \
            + "that the node is likely not ice covered, may or may not be " \
            + "partially or fully ice covered, and likely fully ice covered, " \
            + "respectively."
        ice_clim_f[:] = np.nan_to_num(self.node_data["ice_clim_f"], copy=True, nan=self.INT_FILL)

        ice_dyn_f = dataset.createVariable("ice_dyn_f", "i4", ("nx", "nt"),
            fill_value=self.INT_FILL)
        ice_dyn_f.long_name = "dynamical ice cover flag"
        ice_dyn_f.standard_name = "status_flag"
        ice_dyn_f.source = "Yang et al. (2020)"
        ice_dyn_f.flag_meanings = "no_ice_cover uncertain_ice_cover full_ice_cover"
        ice_dyn_f.flag_values = "0 1 2"
        ice_dyn_f.valid_min = 0
        ice_dyn_f.valid_max = 2
        ice_dyn_f.comment = "Dynamic ice cover flag indicating whether " \
            + "the surface is ice-covered on the day of the observation " \
            + "based on analysis of external satellite optical data. Values " \
            + "of 0, 1, and 2 indicate that the node is not ice covered, " \
            + "partially ice covered, and fully ice covered, respectively."
        ice_dyn_f[:] = np.nan_to_num(self.node_data["ice_dyn_f"], copy=True, nan=self.INT_FILL)

        partial_f = dataset.createVariable("partial_f", "i4", ("nx", "nt"),
            fill_value=self.INT_FILL)
        partial_f.long_name = "partial node coverage flag"
        partial_f.standard_name = "status_flag"
        partial_f.flag_meanings = "covered not_covered"
        partial_f.flag_values = "0 1"
        partial_f.valid_min = 0
        partial_f.valid_max = 2
        partial_f.comment = "Flag that indicates only partial node " \
            + "coverage. The flag is 0 if at least 10 pixels have a valid " \
            + "WSE measurement; the flag is 1 otherwise and node-level " \
            + "quantities are not computed."
        partial_f[:] = np.nan_to_num(self.node_data["partial_f"], copy=True, nan=self.INT_FILL)

        n_good_pix = dataset.createVariable("n_good_pix", "i4", ("nx", "nt"),
            fill_value = self.INT_FILL)
        n_good_pix.long_name = "number of pixels that have a valid WSE"
        n_good_pix.units = "1"
        n_good_pix.valid_min = 0
        n_good_pix.valid_max = 100000
        n_good_pix.comment = "Number of pixels assigned to the node that " \
            + "have a valid node WSE."
        n_good_pix[:] = np.nan_to_num(self.node_data["n_good_pix"], copy=True, nan=self.INT_FILL)

        xovr_cal_q = dataset.createVariable("xovr_cal_q", "i4", ("nx", "nt"),
            fill_value=self.INT_FILL)
        xovr_cal_q.long_name = "quality of the cross-over calibration"
        xovr_cal_q.flag_masks = "TBD"
        xovr_cal_q.flag_meanings = "TBD"
        xovr_cal_q.flag_values = "T B D"
        xovr_cal_q.valid_min = 0
        xovr_cal_q.valid_max = 1
        xovr_cal_q.comment = "Quality of the cross-over calibration."
        xovr_cal_q[:] = np.nan_to_num(self.node_data["xovr_cal_q"], copy=True, nan=self.INT_FILL)  

    def __write_reach_vars(self, dataset, reach_id):
        """Create and write reach-level variables to NetCDF4 dataset.
        
        TODO:
        - d_x_area_u max value is larger than PDD max value documentation

        Parameters:
        dataset: netCDF4.Dataset
            reach-level dataset to write variables to
        reach_id: str
            unique reach identifier value
        """

        reach_id_v = dataset.createVariable("reach_id", "i8")
        reach_id_v.long_name = "reach ID from prior river database"
        reach_id_v.comment = "Unique reach identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRT, where " \
            + "C=continent, B=basin, R=reach, T=type."
        reach_id_v.assignValue(int(reach_id))
        
        time = dataset.createVariable("time", "f8", ("nt",))
        time.long_name = "time (UTC)"
        time.calendar = "gregorian"
        time.tai_utc_difference = "[value of TAI-UTC at time of first record]"
        time.leap_second = "YYYY-MM-DD hh:mm:ss"
        time.units = "seconds since 2000-01-01 00:00:00.000"
        time.comment = "Time of measurement in seconds in the UTC time " \
            + "scale since 1 Jan 2000 00:00:00 UTC. [tai_utc_difference] is " \
            + "the difference between TAI and UTC reference time (seconds) " \
            + "for the first measurement of the data set. If a leap second " \
            + "occurs within the data set, the metadata leap_second is set " \
            + "to the UTC time at which the leap second occurs."
        time[:] = self.reach_data["time"]
        
        dxa = dataset.createVariable("d_x_area", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        dxa.long_name = "change in cross-sectional area"
        dxa.units = "m^2"
        dxa.valid_min = -10000000
        dxa.valid_max = 10000000
        dxa.comment = "Change in channel cross sectional area from the value reported in the prior river database."
        dxa[:] = np.nan_to_num(self.reach_data["d_x_area"], copy=True, nan=self.FLOAT_FILL)

        dxa_u = dataset.createVariable("d_x_area_u", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        dxa_u.long_name = "total uncertainty of the change in the cross-sectional area"
        dxa_u.units = "m^2"
        dxa_u.valid_min = 0
        dxa_u.valid_max = 1000000000    # TODO fix to match PDD
        dxa_u.comment = "Total one-sigma uncertainty (random and systematic) in the change in the cross-sectional area."
        dxa_u[:] = np.nan_to_num(self.reach_data["d_x_area_u"], copy=True, nan=self.FLOAT_FILL)

        slope2 = dataset.createVariable("slope2", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        slope2.long_name = "enhanced water surface slope with respect to geoid"
        slope2.units = "m/m"
        slope2.valid_min = -0.001
        slope2.valid_max = 0.1
        slope2.comment = "Enhanced water surface slope relative to the geoid, produced using a smoothing of the node wse. The upstream or downstream direction is defined by the prior river database. A positive slope means that the downstream WSE is lower."
        slope2[:] = np.nan_to_num(self.reach_data["slope2"], copy=True, nan=self.FLOAT_FILL)

        slope2_u = dataset.createVariable("slope2_u", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        slope2_u.long_name = "uncertainty in the enhanced water surface slope"
        slope2_u.units = "m/m"
        slope2_u.valid_min = 0
        slope2_u.valid_max = 0.1
        slope2_u.comment = "Total one-sigma uncertainty (random and systematic) in the enhanced water surface slope, including uncertainties of corrections and variation about the fit."
        slope2_u[:] = np.nan_to_num(self.reach_data["slope2_u"], copy=True, nan=self.FLOAT_FILL)
        
        width = dataset.createVariable("width", "f8", ("nt",), 
            fill_value=self.FLOAT_FILL)
        width.long_name = "reach width"
        width.units = "m"
        width.valid_min = 0.0
        width.valid_max = 100000
        width.comment = "Reach width."
        width[:] = np.nan_to_num(self.reach_data["width"], copy=True, nan=self.FLOAT_FILL)

        width_u = dataset.createVariable("width_u", "f8", ("nt",), 
            fill_value=self.FLOAT_FILL)
        width_u.long_name = "total uncertainty in the reach width"
        width_u.units = "m"
        width_u.valid_min = 0
        width_u.valid_max = 100000
        width_u.comment = "Total one-sigma uncertainty (random and systematic) in the reach width."
        width_u[:] = np.nan_to_num(self.reach_data["width_u"], copy=True, nan=self.FLOAT_FILL)

        wse = dataset.createVariable("wse", "f8", ("nt",), fill_value=self.FLOAT_FILL)
        wse.long_name = "water surface elevation with respect to the geoid"
        wse.units = "m"
        wse.valid_min = -1000
        wse.valid_max = 100000
        wse.comment = "Fitted reach water surface elevation, relative to the provided model of the geoid (geoid_hght), with corrections for media delays (wet and dry troposphere, and ionosphere), crossover correction, and tidal effects (solid_tide, load_tidef, and pole_tide) applied."
        wse[:] = np.nan_to_num(self.reach_data["wse"], copy=True, nan=self.FLOAT_FILL)

        wse_u = dataset.createVariable("wse_u", "f8", ("nt",), fill_value=self.FLOAT_FILL)
        wse_u.long_name = "total uncertainty in the water surface elevation"
        wse_u.units = "m"
        wse_u.valid_min = 0.0
        wse_u.valid_max = 999999
        wse_u.comment = "Total one-sigma uncertainty (random and systematic) in the reach WSE, including uncertainties of corrections, and variation about the fit."
        wse_u[:] = np.nan_to_num(self.reach_data["wse_u"], copy=True, nan=self.FLOAT_FILL)

        reach_q = dataset.createVariable("reach_q", "i4", ("nt",),
            fill_value=self.INT_FILL)
        reach_q.long_name = "summary quality indicator for the reach"
        reach_q.standard_name = "summary quality indicator for the reach"
        reach_q.flag_masks = "TBD"
        reach_q.flag_meanings = "good bad"
        reach_q.flag_values = "0 1"
        reach_q.valid_min = 0
        reach_q.valid_max = 1
        reach_q.comment = "Summary quality indicator for the reach " \
            + "measurement. Values of 0 and 1 indicate nominal (good) " \
            + "and off-nominal (suspect) measurements."
        reach_q[:] = np.nan_to_num(self.reach_data["reach_q"], copy=True, nan=self.INT_FILL)

        dark_frac = dataset.createVariable("dark_frac", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        dark_frac.long_name = "fractional area of dark water"
        dark_frac.units = "1"
        dark_frac.valid_min = -1000
        dark_frac.valid_max = 10000
        dark_frac.comment = "Fraction of reach area_total covered by dark water."
        dark_frac[:] = np.nan_to_num(self.reach_data["dark_frac"], copy=True, nan=self.FLOAT_FILL)

        ice_clim_f = dataset.createVariable("ice_clim_f", "i4", ("nt",),
            fill_value=self.INT_FILL)
        ice_clim_f.long_name = "climatological ice cover flag"
        ice_clim_f.standard_name = "status_flag"
        ice_clim_f.source = "Yang et al. (2020)"
        ice_clim_f.flag_meanings = "no_ice_cover uncertain_ice_cover full_ice_cover"
        ice_clim_f.flag_values = "0 1 2"
        ice_clim_f.valid_min = 0
        ice_clim_f.valid_max = 2
        ice_clim_f.comment = "Climatological ice cover flag indicating " \
            + "whether the reach is ice-covered on the day of the " \
            + "observation based on external climatological information " \
            + "(not the SWOT measurement). Values of 0, 1, and 2 indicate " \
            + "that the reach is likely not ice covered, may or may not be " \
            + "partially or fully ice covered, and likely fully ice covered, " \
            + "respectively."
        ice_clim_f[:] = np.nan_to_num(self.reach_data["ice_clim_f"], copy=True, nan=self.INT_FILL)

        ice_dyn_f = dataset.createVariable("ice_dyn_f", "i4", ("nt",),
            fill_value=self.INT_FILL)
        ice_dyn_f.long_name = "dynamical ice cover flag"
        ice_dyn_f.standard_name = "status_flag"
        ice_dyn_f.source = "Yang et al. (2020)"
        ice_dyn_f.flag_meanings = "no_ice_cover uncertain_ice_cover full_ice_cover"
        ice_dyn_f.flag_values = "0 1 2"
        ice_dyn_f.valid_min = 0
        ice_dyn_f.valid_max = 2
        ice_dyn_f.comment = "Dynamic ice cover flag indicating whether " \
            + "the surface is ice-covered on the day of the observation " \
            + "based on analysis of external satellite optical data. Values " \
            + "of 0, 1, and 2 indicate that the reach is not ice covered, " \
            + "partially ice covered, and fully ice covered, respectively."
        ice_dyn_f[:] = np.nan_to_num(self.reach_data["ice_dyn_f"], copy=True, nan=self.INT_FILL)

        partial_f = dataset.createVariable("partial_f", "i4", ("nt",),
            fill_value=self.INT_FILL)
        partial_f.long_name = "partial reach coverage flag"
        partial_f.standard_name = "status_flag"
        partial_f.flag_meanings = "covered not_covered"
        partial_f.flag_values = "0 1"
        partial_f.valid_min = 0
        partial_f.valid_max = 1
        partial_f.comment = "Flag that indicates only partial reach " \
            + "coverage. The flag is 0 if at least half the nodes of the " \
            + "reach have valid WSE measurements; the flag is 1 otherwise " \
            + "and reach-level quantities are not computed."
        partial_f[:] = np.nan_to_num(self.reach_data["partial_f"], copy=True, nan=self.INT_FILL)

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
        n_good_nod[:] = np.nan_to_num(self.reach_data["n_good_nod"], copy=True, nan=self.INT_FILL)

        obs_frac_n = dataset.createVariable("obs_frac_n", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        obs_frac_n.long_name = "fraction of nodes that have a valid WSE"
        obs_frac_n.units = "1"
        obs_frac_n.valid_min = 0
        obs_frac_n.valid_max = 1
        obs_frac_n.comment = "Fraction of nodes (n_good_nod/p_n_nodes) " \
            + "in the reach that have a valid node WSE. The value is " \
            + "between 0 and 1."
        obs_frac_n[:] = np.nan_to_num(self.reach_data["obs_frac_n"], copy=True, nan=self.INT_FILL)

        xovr_cal_q = dataset.createVariable("xovr_cal_q", "i4", ("nt",),
            fill_value=self.INT_FILL)
        xovr_cal_q.long_name = "quality of the cross-over calibration"
        xovr_cal_q.flag_masks = "TBD"
        xovr_cal_q.flag_meanings = "TBD"
        xovr_cal_q.flag_values = "T B D"
        xovr_cal_q.valid_min = 0
        xovr_cal_q.valid_max = 1
        xovr_cal_q.comment = "Quality of the cross-over calibration."
        xovr_cal_q[:] = np.nan_to_num(self.reach_data["xovr_cal_q"], copy=True, nan=self.INT_FILL)