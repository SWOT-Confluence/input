# Third-party imports
import numpy as np
from netCDF4 import stringtochar

# Local imports
from input.write.WriteStrategy import WriteStrategy

class WriteRiver(WriteStrategy):
    """A class that extends WriteStrategy to write river data to NetCDF.
    
    Intermediate data is stored in NetCDF file format and is taken from various
    AWS S3 buckets.

    Attributes
    ----------
    node_ids: list
        list of string node identifiers

    Methods
    -------
    create_dimensions( nx, nt, dataset)
        create dimensions and coordinate variables for dataset
    write_data()
        writes SWOT data dictionaries to NetCDF files organized by continent
    __write_node_vars(dataset, reach_id, node_ids)
        writes node level data to NetCDF file in node group
    __write_reach_vars(dataset, reach_id)
        writes reach level data to NetCDF file in reach group
    """

    def __init__(self, swot_id, output_dir, node_ids):
        """
        Parameters
        ----------
        swot_id: int
           unique SWOT identifier
        output_dir: Path
            path to output directory on EFS 'input' mount
        node_ids: list
            list of string node identifiers
        """
        
        super().__init__(swot_id, output_dir)
        self.node_ids = node_ids

    def create_dimensions(self, dataset, obs_times):
        """Create dimensions and coordinate variables for dataset.
        
        Parameters
        ----------
        dataset: netCDF4.Dataset
            dataset to write node level data to
        obs_times: list
            list of string cycle/pass identifiers
        """

         # Create dimension(s)
        dataset.createDimension("nt", len(obs_times))
        dataset.createDimension("nx", len(self.node_ids))

        # Create coordinate variable(s)
        nt_v = dataset.createVariable("nt", "i4", ("nt",))
        nt_v.units = "pass"
        nt_v.long_name = "time steps"
        nt_v[:] = range(len(obs_times))

        nx_v = dataset.createVariable("nx", "i4", ("nx",))
        nx_v.units = "node"
        nx_v.long_name = "number of nodes"
        nx_v[:] = range(1, len(self.node_ids) + 1)

    def write_data(self, dataset, data):
        """Writes node and reach level SWOT data to NetCDF format.
        
        TODO:
        - Figure out maximum cycle/pass length.
        
        Parameters
        ----------
        dataset: netCDF4.Dataset
            netCDF4 dataset to set global attirbutes for
        data: dict
            dictionary of SWOT data variables
        """

        reach_group = dataset.createGroup("reach")
        self.__write_reach_vars(reach_group, data, self.swot_id)
        node_group = dataset.createGroup("node")
        self.__write_node_vars(node_group, data, self.swot_id, self.node_ids)
        
    def __write_node_vars(self, dataset, data, reach_id, node_ids):
        """Create and write reach-level variables to NetCDF4 dataset.

        TODO:
        - d_x_area_u max value is larger than PDD max value documentation
        
        Parameters:
        dataset: netCDF4.Dataset
            reach-level dataset to write variables to
        data: dict
            dictionary of SWOT data variables
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

        node_ids_v = dataset.createVariable("node_id", "i8", ("nx",))
        node_ids_v.long_name = "node ID of the node in the prior river database"
        node_ids_v.comment = "Unique node identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRNNNT, " \
            + "where C=continent, B=basin, R=reach, N=node, T=type."
        node_ids_v[:] = np.array(node_ids, dtype=np.int64)
        
        time = dataset.createVariable("time", "f8", ("nx", "nt"), 
            fill_value=self.FLOAT_FILL)
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
        data["node"]["time"][np.isclose(data["node"]["time"], -999999999999)] = self.FLOAT_FILL    # sac-specific
        time[:] = np.nan_to_num(data["node"]["time"], copy=True, nan=self.FLOAT_FILL)
        
        dataset.createDimension('chartime', 20)
        time_str = dataset.createVariable("time_str", "S1", ("nx", "nt", "chartime"), 
                                          fill_value=self.STR_FILL)
        time_str.long_name = "UTC time"
        time_str.standard_name = "time"
        time_str.calendar = "gregorian"
        time_str.tai_utc_difference = "[value of TAI-UTC at time of first record]"
        time_str.leap_second = "YYYY-MM-DD hh:mm:ss"
        time_str.comment = "Time string giving UTC time. The format is " \
            + "YYYY-MM-DDThh:mm:ssZ, where the Z suffix indicates UTC time."
        time_str[:] = stringtochar(data["node"]["time_str"])

        dxa = dataset.createVariable("d_x_area", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        dxa.long_name = "change in cross-sectional area"
        dxa.units = "m^2"
        dxa.valid_min = -10000000
        dxa.valid_max = 10000000
        dxa.comment = "Change in channel cross sectional area from the " \
            + "value reported in the prior river database. Extracted from " \
            + "reach-level and appended to node."
        data["node"]["d_x_area"][np.isclose(data["node"]["d_x_area"], -1.e+12)] = np.nan    # sac-specific
        dxa[:] = np.nan_to_num(data["node"]["d_x_area"], copy=True, nan=self.FLOAT_FILL)

        dxa_u = dataset.createVariable("d_x_area_u", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        dxa_u.long_name = "total uncertainty of the change in the cross-sectional area"
        dxa_u.units = "m^2"
        dxa_u.valid_min = 0
        dxa_u.valid_max = 10000000
        dxa_u.comment = "Total one-sigma uncertainty (random and systematic) " \
            + "in the change in the cross-sectional area. Extracted from " \
            + "reach-level and appended to node."
        dxa_u[:] = np.nan_to_num(data["node"]["d_x_area_u"], copy=True, nan=self.FLOAT_FILL)
        
        slope = dataset.createVariable("slope", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        slope.long_name = "water surface slope with respect to the geoid"
        slope.units = "m/m"
        slope.valid_min = -0.001
        slope.valid_max = 0.1
        slope.comment = "Fitted water surface slope relative to the geoid, " \
            + "and with the same corrections and geophysical fields applied as " \
            + "wse. The units are m/m. The upstream or downstream direction " \
            + "is defined by the prior river database. A positive slope " \
            + "means that the downstream WSE is lower."
        slope[:] = np.nan_to_num(data["node"]["slope"], copy=True, nan=self.FLOAT_FILL)

        slope_u = dataset.createVariable("slope_u", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        slope_u.long_name = "total uncertainty in the water surface slope"
        slope_u.units = "m/m"
        slope_u.valid_min = 0
        slope_u.valid_max = 0.1
        slope_u.comment = "Total one-sigma uncertainty (random and " \
            + "systematic) in the water surface slope, including " \
            + "uncertainties of corrections and variation about the fit."
        slope_u[:] = np.nan_to_num(data["node"]["slope_u"], copy=True, nan=self.FLOAT_FILL)

        slope2 = dataset.createVariable("slope2", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        slope2.long_name = "enhanced water surface slope with respect to geoid"
        slope2.units = "m/m"
        slope2.valid_min = -0.001
        slope2.valid_max = 0.1
        slope2.comment = "Enhanced water surface slope relative to the " \
            + "geoid, produced using a smoothing of the node wse. The " \
            + "upstream or downstream direction is defined by the prior " \
            + "river database. A positive slope means that the downstream " \
            + "WSE is lower. Extracted from reach-level and appended to node."
        slope2[:] = np.nan_to_num(data["node"]["slope2"], copy=True, nan=self.FLOAT_FILL)

        slope2_u = dataset.createVariable("slope2_u", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        slope2_u.long_name = "uncertainty in the enhanced water surface slope"
        slope2_u.units = "m/m"
        slope2_u.valid_min = 0
        slope2_u.valid_max = 0.1
        slope2_u.comment = "Total one-sigma uncertainty (random and " \
            + "systematic) in the enhanced water surface slope, including " \
            + "uncertainties of corrections and variation about the fit. " \
            + "Extracted from reach-level and appended to node."
        slope2_u[:] = np.nan_to_num(data["node"]["slope2_u"], copy=True, nan=self.FLOAT_FILL)

        width = dataset.createVariable("width", "f8", ("nx", "nt"), 
            fill_value = self.FLOAT_FILL)
        width.long_name = "node width"
        width.units = "m"
        width.valid_min = 0.0
        width.valid_max = 100000
        width.comment = "Node width."
        width[:] = np.nan_to_num(data["node"]["width"], copy=True, nan=self.FLOAT_FILL)

        width_u = dataset.createVariable("width_u", "f8", ("nx", "nt"), 
            fill_value = self.FLOAT_FILL)
        width_u.long_name = "total uncertainty in the node width"
        width_u.units = "m"
        width_u.valid_min = 0
        width_u.valid_max = 100000
        width_u.comment = "Total one-sigma uncertainty (random and systematic) in the node width."
        width_u[:] = np.nan_to_num(data["node"]["width_u"], copy=True, nan=self.FLOAT_FILL)

        wse = dataset.createVariable("wse", "f8", ("nx", "nt"), fill_value = self.FLOAT_FILL)
        wse.long_name = "water surface elevation with respect to the geoid"
        wse.units = "m"
        wse.valid_min = -1000
        wse.valid_max = 100000
        wse.comment = "Fitted node water surface elevation, relative to the " \
            + "provided model of the geoid (geoid_hght), with all " \
            + "corrections for media delays (wet and dry troposphere, and " \
            +" ionosphere), crossover correction, and tidal effects " \
            + "(solid_tide, load_tidef, and pole_tide) applied."
        wse[:] = np.nan_to_num(data["node"]["wse"], copy=True, nan=self.FLOAT_FILL)
        
        wse_u = dataset.createVariable("wse_u", "f8", ("nx", "nt"), fill_value = self.FLOAT_FILL)
        wse_u.long_name = "total uncertainty in the water surface elevation"
        wse_u.units = "m"
        wse_u.valid_min = 0.0
        wse_u.valid_max = 999999
        wse_u.comment = "Total one-sigma uncertainty (random and systematic) " \
            + "in the node WSE, including uncertainties of corrections, and " \
            + "variation about the fit."
        wse_u[:] = np.nan_to_num(data["node"]["wse_u"], copy=True, nan=self.FLOAT_FILL)

        node_q = dataset.createVariable("node_q", "i4", ("nx", "nt"), 
            fill_value=self.INT_FILL)
        node_q.long_name = "summary quality indicator for the node"
        node_q.standard_name = "status_flag"
        node_q.short_name = "node_qual"
        node_q.flag_meanings = "good suspect degraded bad"
        node_q.flag_values = "0 1 2 3"
        node_q.valid_min = 0
        node_q.valid_max = 3
        node_q.comment = "Summary quality indicator for the node " \
            + "measurement. Value of 0 indicates a nominal measurement, 1 " \
            + "indicates a suspect measurement, 2 indicates a degraded " \
                + "quality measurement, and 3 indicates a bad measurement."
        node_q[:] = np.nan_to_num(data["node"]["node_q"], copy=True, nan=self.INT_FILL)
        
        node_q_b = dataset.createVariable("node_q_b", "i4", ("nx", "nt"),
            fill_value=self.INT_FILL)
        node_q_b.long_name = "bitwise quality indicator for the node"
        node_q_b.standard_name = "status_flag"
        node_q_b.short_name = "node_qual_bitwise"
        node_q_b.flag_meanings = "sig0_qual_suspect classification_qual_suspect " \
            + "geolocation_qual_suspect water_fraction_suspect blocking_width_suspect " \
            + "bright_land few_sig0_observations few_area_observations " \
            + "few_wse_observations far_range_suspect near_range_suspect " \
            + "classification_qual_degraded geolocation_qual_degraded wse_outlier " \
            + "wse_bad no_sig0_observations no_area_observations no_wse_observations no_observations"
        node_q_b.flag_masks = "1 2 4 8 16 128 512 1024 2048 8192 16384 262144 524288 4194304 8388608 16777216 33554432 67108864 134217728 268435456"
        node_q_b.valid_min = 0
        node_q_b.valid_max = 533491359
        node_q_b.comment = "Bitwise quality indicator for the node " \
            + "measurement. If this word is interpreted as an unsigned " \
            + "integer, a value of 0 indicates good data, values greater " \
            + "than 0 but less than 262144 represent suspect data, values " \
            + "greater than or equal to 262144 but less than 4194304 " \
            + "represent degraded data, and values greater than or equal to " \
            + "4194304 represent bad data."
        node_q_b[:] = np.nan_to_num(data["node"]["node_q_b"], copy=True, nan=self.INT_FILL)

        dark_frac = dataset.createVariable("dark_frac", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        dark_frac.long_name = "fractional area of dark water"
        dark_frac.units = "1"
        dark_frac.valid_min = 0
        dark_frac.valid_max = 1
        dark_frac.comment = "Fraction of node area_total covered by dark water."
        dark_frac[:] = np.nan_to_num(data["node"]["dark_frac"], copy=True, nan=self.FLOAT_FILL)

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
        ice_clim_f[:] = np.nan_to_num(data["node"]["ice_clim_f"], copy=True, nan=self.INT_FILL)

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
        ice_dyn_f[:] = np.nan_to_num(data["node"]["ice_dyn_f"], copy=True, nan=self.INT_FILL)

        n_good_pix = dataset.createVariable("n_good_pix", "i4", ("nx", "nt"),
            fill_value = self.INT_FILL)
        n_good_pix.long_name = "number of pixels that have a valid WSE"
        n_good_pix.units = "1"
        n_good_pix.valid_min = 0
        n_good_pix.valid_max = 100000
        n_good_pix.comment = "Number of pixels assigned to the node that " \
            + "have a valid node WSE."
        data["node"]["n_good_pix"][data["node"]["n_good_pix"] == -99999999] = self.INT_FILL    # sac-specific
        n_good_pix[:] = np.nan_to_num(data["node"]["n_good_pix"], copy=True, nan=self.INT_FILL)

        xovr_cal_q = dataset.createVariable("xovr_cal_q", "i4", ("nx", "nt"),
            fill_value=self.INT_FILL)
        xovr_cal_q.long_name = "quality of the cross-over calibration"
        xovr_cal_q.flag_meanings = "good suspect bad"
        xovr_cal_q.flag_values = "0 1 2"
        xovr_cal_q.valid_min = 0
        xovr_cal_q.valid_max = 2
        xovr_cal_q.comment = "Quality of the cross-over calibration. A value " \
            + "of 0 indicates a nominal measurement, 1 indicates a suspect " \
            + "measurement, and 2 indicates a bad measurement."
        xovr_cal_q[:] = np.nan_to_num(data["node"]["xovr_cal_q"], copy=True, nan=self.INT_FILL)
        
        xtrk_dist = dataset.createVariable("xtrk_dist", "f8", ("nx", "nt"),
            fill_value=self.FLOAT_FILL)
        xtrk_dist.long_name = "distance to the satellite ground track"
        xtrk_dist.short_name = "cross_track_distance"
        xtrk_dist.units = "m"
        xtrk_dist.valid_min = -75000
        xtrk_dist.valid_max = 75000
        xtrk_dist.comment = "Distance of the observed node location from the " \
            + "spacecraft nadir track. A negative value indicates the left side " \
            + "of the swath, relative to the spacecraft velocity vector. A " \
            + "positive value indicates the right side of the swath."
        xtrk_dist[:] = np.nan_to_num(data["node"]["xtrk_dist"], copy=True, nan=self.FLOAT_FILL)

    def __write_reach_vars(self, dataset, data, reach_id):
        """Create and write reach-level variables to NetCDF4 dataset.
        
        TODO:
        - d_x_area_u max value is larger than PDD max value documentation

        Parameters:
        dataset: netCDF4.Dataset
            reach-level dataset to write variables to
        data: dict
            dictionary of SWOT data variables
        reach_id: str
            unique reach identifier value
        """

        reach_id_v = dataset.createVariable("reach_id", "i8")
        reach_id_v.long_name = "reach ID from prior river database"
        reach_id_v.comment = "Unique reach identifier from the prior river " \
            + "database. The format of the identifier is CBBBBBRRRRT, where " \
            + "C=continent, B=basin, R=reach, T=type."
        reach_id_v.assignValue(int(reach_id))
        
        time = dataset.createVariable("time", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
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
        data["reach"]["time"][np.isclose(data["reach"]["time"], -999999999999)] = self.FLOAT_FILL    # sac-specific
        time[:] = np.nan_to_num(data["reach"]["time"], copy=True, nan=self.FLOAT_FILL)
        
        dataset.createDimension('chartime', 20)
        time_str = dataset.createVariable("time_str", "S1", ("nt", "chartime"), 
                                          fill_value=self.STR_FILL)
        time_str.long_name = "UTC time"
        time_str.standard_name = "time"
        time_str.calendar = "gregorian"
        time_str.tai_utc_difference = "[value of TAI-UTC at time of first record]"
        time_str.leap_second = "YYYY-MM-DD hh:mm:ss"
        time_str.comment = "Time string giving UTC time. The format is " \
            + "YYYY-MM-DDThh:mm:ssZ, where the Z suffix indicates UTC time."
        time_str[:] = stringtochar(data["reach"]["time_str"].astype("S20"))
        
        dxa = dataset.createVariable("d_x_area", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        dxa.long_name = "change in cross-sectional area"
        dxa.units = "m^2"
        dxa.valid_min = -10000000
        dxa.valid_max = 10000000
        dxa.comment = "Change in channel cross sectional area from the value " \
            + "reported in the prior river database."
        data["reach"]["d_x_area"][np.isclose(data["reach"]["d_x_area"], -1.e+12)] = np.nan    # sac-specific
        dxa[:] = np.nan_to_num(data["reach"]["d_x_area"], copy=True, nan=self.FLOAT_FILL)

        dxa_u = dataset.createVariable("d_x_area_u", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        dxa_u.long_name = "total uncertainty of the change in the cross-sectional area"
        dxa_u.units = "m^2"
        dxa_u.valid_min = 0
        dxa_u.valid_max = 10000000    # TODO fix to match PDD
        dxa_u.comment = "Total one-sigma uncertainty (random and systematic) " \
            + "in the change in the cross-sectional area."
        dxa_u[:] = np.nan_to_num(data["reach"]["d_x_area_u"], copy=True, nan=self.FLOAT_FILL)
        
        slope = dataset.createVariable("slope", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        slope.long_name = "water surface slope with respect to the geoid"
        slope.units = "m/m"
        slope.valid_min = -0.001
        slope.valid_max = 0.1
        slope.comment = "Fitted water surface slope relative to the geoid, " \
            + "and with the same corrections and geophysical fields applied as " \
            + "wse. The units are m/m. The upstream or downstream direction " \
            + "is defined by the prior river database. A positive slope " \
            + "means that the downstream WSE is lower."
        slope[:] = np.nan_to_num(data["reach"]["slope"], copy=True, nan=self.FLOAT_FILL)

        slope_u = dataset.createVariable("slope_u", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        slope_u.long_name = "total uncertainty in the water surface slope"
        slope_u.units = "m/m"
        slope_u.valid_min = 0
        slope_u.valid_max = 0.1
        slope_u.comment = "Total one-sigma uncertainty (random and " \
            + "systematic) in the water surface slope, including " \
            + "uncertainties of corrections and variation about the fit."
        slope_u[:] = np.nan_to_num(data["reach"]["slope_u"], copy=True, nan=self.FLOAT_FILL)

        slope2 = dataset.createVariable("slope2", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        slope2.long_name = "enhanced water surface slope with respect to geoid"
        slope2.units = "m/m"
        slope2.valid_min = -0.001
        slope2.valid_max = 0.1
        slope2.comment = "Enhanced water surface slope relative to the " \
            + "geoid, produced using a smoothing of the node wse. The " \
            + "upstream or downstream direction is defined by the prior " \
            + "river database. A positive slope means that the downstream " \
            + "WSE is lower."
        slope2[:] = np.nan_to_num(data["reach"]["slope2"], copy=True, nan=self.FLOAT_FILL)

        slope2_u = dataset.createVariable("slope2_u", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        slope2_u.long_name = "uncertainty in the enhanced water surface slope"
        slope2_u.units = "m/m"
        slope2_u.valid_min = 0
        slope2_u.valid_max = 0.1
        slope2_u.comment = "Total one-sigma uncertainty (random and " \
            + "systematic) in the enhanced water surface slope, including " \
            + "uncertainties of corrections and variation about the fit."
        slope2_u[:] = np.nan_to_num(data["reach"]["slope2_u"], copy=True, nan=self.FLOAT_FILL)
        
        width = dataset.createVariable("width", "f8", ("nt",), 
            fill_value=self.FLOAT_FILL)
        width.long_name = "reach width"
        width.units = "m"
        width.valid_min = 0.0
        width.valid_max = 100000
        width.comment = "Reach width."
        width[:] = np.nan_to_num(data["reach"]["width"], copy=True, nan=self.FLOAT_FILL)

        width_u = dataset.createVariable("width_u", "f8", ("nt",), 
            fill_value=self.FLOAT_FILL)
        width_u.long_name = "total uncertainty in the reach width"
        width_u.units = "m"
        width_u.valid_min = 0
        width_u.valid_max = 100000
        width_u.comment = "Total one-sigma uncertainty (random and systematic) in the reach width."
        width_u[:] = np.nan_to_num(data["reach"]["width_u"], copy=True, nan=self.FLOAT_FILL)

        wse = dataset.createVariable("wse", "f8", ("nt",), fill_value=self.FLOAT_FILL)
        wse.long_name = "water surface elevation with respect to the geoid"
        wse.units = "m"
        wse.valid_min = -1500
        wse.valid_max = 150000
        wse.comment = "Fitted reach water surface elevation, relative to the " \
            + "provided model of the geoid (geoid_hght), with corrections " \
            + "for media delays (wet and dry troposphere, and ionosphere), " \
            + "crossover correction, and tidal effects (solid_tide, " \
            + "load_tidef, and pole_tide) applied."
        wse[:] = np.nan_to_num(data["reach"]["wse"], copy=True, nan=self.FLOAT_FILL)

        wse_u = dataset.createVariable("wse_u", "f8", ("nt",), fill_value=self.FLOAT_FILL)
        wse_u.long_name = "total uncertainty in the water surface elevation"
        wse_u.units = "m"
        wse_u.valid_min = 0.0
        wse_u.valid_max = 999999
        wse_u.comment = "Total one-sigma uncertainty (random and systematic) " \
            + "in the reach WSE, including uncertainties of corrections, and " \
            + "variation about the fit."
        wse_u[:] = np.nan_to_num(data["reach"]["wse_u"], copy=True, nan=self.FLOAT_FILL)

        reach_q = dataset.createVariable("reach_q", "i4", ("nt",),
            fill_value=self.INT_FILL)
        reach_q.long_name = "summary quality indicator for the reach"
        reach_q.standard_name = "summary quality indicator for the reach"
        reach_q.flag_meanings = "good suspect degraded bad"
        reach_q.flag_values = "0 1 2 3"
        reach_q.valid_min = 0
        reach_q.valid_max = 3
        reach_q.comment = "Summary quality indicator for the reach " \
            + "measurement. A value of 0 indicates a nominal measurement, 1 " \
            + "indicates a suspect measurement, 2 indicates a degraded " \
            + "measurement, and 3 indicates a bad measurement."
        reach_q[:] = np.nan_to_num(data["reach"]["reach_q"], copy=True, nan=self.INT_FILL)
        
        reach_q_b = dataset.createVariable("reach_q_b", "i4", ("nx", "nt"),
            fill_value=self.INT_FILL)
        reach_q_b.long_name = "bitwise quality indicator for the reach"
        reach_q_b.standard_name = "status_flag"
        reach_q_b.short_name = "reach_qual_bitwise"
        reach_q_b.flag_meanings = "classification_qual_suspect geolocation_qual_suspect water_fraction_suspect" \
            + "bright_land few_area_observations few_wse_observations far_range_suspect" \
            + "near_range_suspect partially_observed classification_qual_degraded" \
            + "geolocation_qual_degraded lake_flagged below_min_fit_points no_area_observations" \
            + "no_wse_observations no_observations"
        reach_q_b.flag_masks = "2 4 8 128 1024 2048 8192 16384 32768 262144 524288 4194304 33554432 67108864 134217728 268435456"
        reach_q_b.valid_min = 0
        reach_q_b.valid_max = 508357774
        reach_q_b.comment = "Bitwise quality indicator for the reach measurements. " \
            + "If this word is interpreted as an unsigned integer, a value of 0 " \
            + "indicates good data, values greater than 0 but less than 262144 " \
            + "represent suspect data, values greater than or equal to 262144 " \
            + "but less than 4194304 represent degraded data, and values greater " \
            + "than or equal to 4194304 represent bad data."
        reach_q_b[:] = np.nan_to_num(data["reach"]["reach_q_b"], copy=True, nan=self.INT_FILL)

        dark_frac = dataset.createVariable("dark_frac", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        dark_frac.long_name = "fractional area of dark water"
        dark_frac.units = "1"
        dark_frac.valid_min = -1000
        dark_frac.valid_max = 10000
        dark_frac.comment = "Fraction of reach area_total covered by dark water."
        dark_frac[:] = np.nan_to_num(data["reach"]["dark_frac"], copy=True, nan=self.FLOAT_FILL)

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
        ice_clim_f[:] = np.nan_to_num(data["reach"]["ice_clim_f"], copy=True, nan=self.INT_FILL)

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
        ice_dyn_f[:] = np.nan_to_num(data["reach"]["ice_dyn_f"], copy=True, nan=self.INT_FILL)

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
        partial_f[:] = np.nan_to_num(data["reach"]["partial_f"], copy=True, nan=self.INT_FILL)

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
        n_good_nod[:] = np.nan_to_num(data["reach"]["n_good_nod"], copy=True, nan=self.INT_FILL)

        obs_frac_n = dataset.createVariable("obs_frac_n", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        obs_frac_n.long_name = "fraction of nodes that have a valid WSE"
        obs_frac_n.units = "1"
        obs_frac_n.valid_min = 0
        obs_frac_n.valid_max = 1
        obs_frac_n.comment = "Fraction of nodes (n_good_nod/p_n_nodes) " \
            + "in the reach that have a valid node WSE. The value is " \
            + "between 0 and 1."
        obs_frac_n[:] = np.nan_to_num(data["reach"]["obs_frac_n"], copy=True, nan=self.INT_FILL)

        xovr_cal_q = dataset.createVariable("xovr_cal_q", "i4", ("nt",),
            fill_value=self.INT_FILL)
        xovr_cal_q.long_name = "quality of the cross-over calibration"
        xovr_cal_q.flag_meanings = "good suspect bad"
        xovr_cal_q.flag_values = "0 1 2"
        xovr_cal_q.valid_min = 0
        xovr_cal_q.valid_max = 2
        xovr_cal_q.comment = "Quality of the cross-over calibration. A value " \
            + "of 0 indicates a nominal measurement, 1 indicates a suspect " \
            + "measurement, and 2 indicates a bad measurement."
        xovr_cal_q[:] = np.nan_to_num(data["reach"]["xovr_cal_q"], copy=True, nan=self.INT_FILL)
        
        xtrk_dist = dataset.createVariable("xtrk_dist", "f8", ("nt",),
            fill_value=self.FLOAT_FILL)
        xtrk_dist.long_name = "distance to the satellite ground track"
        xtrk_dist.short_name = "cross_track_distance"
        xtrk_dist.units = "m"
        xtrk_dist.valid_min = -75000
        xtrk_dist.valid_max = 75000
        xtrk_dist.comment = "Average distance of the observed node locations " \
            + "in the reach from the spacecraft nadir track. A negative value " \
            + "indicates the left side of the swath, relative to the spacecraft " \
            + "velocity vector. A positive value indicates the right side of the swath."
        xtrk_dist[:] = np.nan_to_num(data["reach"]["xtrk_dist"], copy=True, nan=self.FLOAT_FILL)