# Standard imports
import glob
from os import scandir
from pathlib import Path
from shutil import move, rmtree
import tempfile

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np

class GageAppend:
    """Class that appends USGS gage data to the SoS.
    
    Attributes
    ----------
    FLOAT_FILL: float
        Fill value for any missing float values in mapped GRDC data
    INT_FILL: int
        Fill value for any missing integer values in mapped GRDC data
    map_dict: dict
        Dict organized by continent with reach_id and GRADES discharge data
    sos_dict: dict
        Dict organized by continent reach_ids and SoS file name data
    sos_dir: Path
        Path to SoS directory
    temp_sos: TemporaryDirectory
        Temporary directory that holds old SoS version
    usgs_dict: dict
        Dictionary of USGS data

    Methods
    -------
    append()
        Appends data to the SoS
    __append_geobam(temp, sos)
        Append geoBAM results
    __append_moi(temp, sos)
        Append MOI results
    __append_pd(temp, sos)
        Append postdiagnostics data
    __append_usgs_data(sos, continent)
        Append USGS data to the SoS file
    __copy_past_results(temp, sos)
        Copy results of past Confluence run
    __copy_sos(temp, sos)
        Copy SoS data to new SoS file
    map_data()
        Maps USGS data to SoS data organized by continent.
    mv_sos()
        Move SoS file to temporary directory
    read_sos()
        Reads in the SoS data and stores it in a dict organized by continent.
    """

    FLOAT_FILL = -999999999999
    INT_FILL = -999

    def __init__(self, sos_dir, usgs_dict):
        """
        Parameters
        ----------
        sos_dir: Path
            Path to SoS directory
        temp_sos: TemporaryDirectory
            Temporary directory that holds old SoS version
        usgs_dict: dict
            Dictionary of USGS data
        """

        self.sos_dir = sos_dir
        self.temp_sos = None
        self.usgs_dict = usgs_dict
        self.map_dict = { "af": {}, "as": {}, "eu": {}, "na": {},
            "oc": {}, "sa": {} }
        self.sos_dict = { "af": None, "as": None, "eu": None, "na": None,
            "oc": None, "sa": None }

    def append_data(self):
        """Appends data to the SoS.
        
        Data is copied from temporary SoS file and extracted USGS data.
        """

        # Move SoS
        self.temp_sos = self.mv_sos()

        # Copy data from SoS files to new files and append USGS data
        for continent in self.sos_dict.keys():
            if self.sos_dict[continent]:
                temp_file = glob.glob(f"{self.temp_sos.name}/{continent}*")[0]
                temp = Dataset(temp_file, 'r')

                sos_file = f"{self.sos_dir}/{continent}_apriori_rivers_v07_SOS.nc"
                sos = Dataset(sos_file, 'w')

                self.__copy_sos(temp, sos)
                sos.createGroup("reaches")
                self.__copy_sos(temp["reaches"], sos["reaches"])
                sos.createGroup("nodes")
                self.__copy_sos(temp["nodes"], sos["nodes"])
                sos.createGroup("model")
                self.__copy_sos(temp["model"], sos["model"])
                sos["model"].createGroup("grdc")
                self.__copy_sos(temp["model"]["grdc"], sos["model"]["grdc"])

                if temp.version != "0000":
                    self.__copy_past_results(temp, sos)

                if self.map_dict[continent]:
                    self.__append_usgs_data(sos, continent)           

                temp.close()
                sos.close()
        
        # Remove temporary directory
        rmtree(Path(self.temp_sos.name))

    def __append_geobam(self, temp, sos):
        """Append geoBAM results.
        
        Parameters
        ----------
        temp: Dataset or Group
            temporary NetCDF4 Dataset 
        sos: Dataset
            new NetCDF4 Dataset
        """

        sos.createGroup("geobam")
        sos["geobam"].createGroup("logQ")
        self.__copy_sos(temp["geobam"]["logQ"], sos["geobam"]["logQ"])
        sos["geobam"].createGroup("logWc")
        self.__copy_sos(temp["geobam"]["logWc"], sos["geobam"]["logWc"])
        sos["geobam"].createGroup("logQc")
        self.__copy_sos(temp["geobam"]["logQc"], sos["geobam"]["logQc"])
        sos["geobam"].createGroup("logn_man")
        self.__copy_sos(temp["geobam"]["logn_man"], sos["geobam"]["logn_man"])
        sos["geobam"].createGroup("logn_amhg")
        self.__copy_sos(temp["geobam"]["logn_amhg"], sos["geobam"]["logn_amhg"])
        sos["geobam"].createGroup("A0")
        self.__copy_sos(temp["geobam"]["A0"], sos["geobam"]["A0"])
        sos["geobam"].createGroup("b")
        self.__copy_sos(temp["geobam"]["b"], sos["geobam"]["b"])
        sos["geobam"].createGroup("logr")
        self.__copy_sos(temp["geobam"]["logr"], sos["geobam"]["logr"])
        sos["geobam"].createGroup("logWb")
        self.__copy_sos(temp["geobam"]["logWb"], sos["geobam"]["logWb"])
        sos["geobam"].createGroup("logDb")
        self.__copy_sos(temp["geobam"]["logDb"], sos["geobam"]["logDb"])

    def __append_moi(self, temp, sos):
        """Append MOI results.
        
        Parameters
        ----------
        temp: Dataset or Group
            temporary NetCDF4 Dataset 
        sos: Dataset
            new NetCDF4 Dataset
        """

        sos.createGroup("moi")
        sos["moi"].createGroup("geobam")
        self.__copy_sos(temp["moi"]["geobam"], sos["moi"]["geobam"])
        sos["moi"].createGroup("hivdi")
        self.__copy_sos(temp["moi"]["hivdi"], sos["moi"]["hivdi"])
        sos["moi"].createGroup("metroman")
        self.__copy_sos(temp["moi"]["metroman"], sos["moi"]["metroman"])
        sos["moi"].createGroup("momma")
        self.__copy_sos(temp["moi"]["momma"], sos["moi"]["momma"])

    def __append_pd(self, temp, sos):
        """Append postdiagnostics data.
        
        Parameters
        ----------
        temp: Dataset or Group
            temporary NetCDF4 Dataset 
        sos: Dataset
            new NetCDF4 Dataset
        """

        sos.createGroup("postdiagnostics")
        self.__copy_sos(temp["postdiagnostics"], sos["postdiagnostics"])
        sos["postdiagnostics"].createGroup("basin")
        self.__copy_sos(temp["postdiagnostics"]["basin"], sos["postdiagnostics"]["basin"])
        sos["postdiagnostics"].createGroup("reach")
        self.__copy_sos(temp["postdiagnostics"]["reach"], sos["postdiagnostics"]["reach"])


    def __append_usgs_data(self, sos, continent):
        """Append USGS data to the SoS file.

        Data is stored in a group labelled usgs nested under model.

        Parameters
        ----------
        sos: Dataset
            new NetCDF4 Dataset
        continent: str
            string abbreviation of continent data is for
        """

        # USGS data - NEW
        usgs = sos["model"].createGroup("usgs")

        usgs.createDimension("num_days", self.map_dict[continent]["days"].shape[0])
        dt = usgs.createVariable("num_days", "i4", ("num_days", ))
        dt.units = "day"
        dt[:] = self.map_dict[continent]["days"]

        usgs.createDimension("num_usgs_reaches", self.map_dict[continent]["usgs_reach_id"].shape[0])
        usgs_reach_id = usgs.createVariable("usgs_reach_id", "i8", ("num_usgs_reaches", ))
        usgs_reach_id.format = "CBBBBBRRRRT"
        usgs_reach_id[:] = self.map_dict[continent]["usgs_reach_id"]

        fdq = usgs.createVariable("flow_duration_q", "f8", ("num_usgs_reaches", "probability"), fill_value=self.FLOAT_FILL)
        fdq.long_name = "USGS flow_Duration_curve_discharge"
        fdq.comment = "USGS discharge values from the flow duration curve for this cell"
        fdq.units = "m^3/s"
        fdq[:] = np.nan_to_num(self.map_dict[continent]["fdq"], copy=True, nan=self.FLOAT_FILL)

        max_q = usgs.createVariable("max_q", "f8", ("num_usgs_reaches",), fill_value=self.FLOAT_FILL)
        max_q.long_name = "USGS maximum_discharge"
        max_q.comment = "USGS highest discharge value in this cell"
        max_q.units = "m^3/s"
        max_q[:] = np.nan_to_num(self.map_dict[continent]["max_q"], copy=True, nan=self.FLOAT_FILL)

        monthly_q = usgs.createVariable("monthly_q", "f8", ("num_usgs_reaches", "num_months"), fill_value=self.FLOAT_FILL)
        monthly_q.long_name = "USGS mean_monthly_discharge"
        monthly_q.comment = "USGS monthly mean discharge time series in this cell"
        monthly_q.units = "m^3/s"
        monthly_q[:] = np.nan_to_num(self.map_dict[continent]["monthly_q"], copy=True, nan=self.FLOAT_FILL)

        mean_q = usgs.createVariable("mean_q", "f8", ("num_usgs_reaches",), fill_value=self.FLOAT_FILL)
        mean_q.long_name = "USGS mean_discharge"
        mean_q.comment = "USGS mean discharge value in this cell"
        mean_q.units = "m^3/s"
        mean_q[:] = np.nan_to_num(self.map_dict[continent]["mean_q"], copy=True, nan=self.FLOAT_FILL)

        min_q = usgs.createVariable("min_q", "f8", ("num_usgs_reaches",), fill_value=self.FLOAT_FILL)
        min_q.long_name = "USGS minimum_discharge"
        min_q.comment = "USGS lowest discharge value in this cell"
        min_q.units = "m^3/s"
        min_q[:] = np.nan_to_num(self.map_dict[continent]["min_q"], copy=True, nan=self.FLOAT_FILL)
        
        tyr = usgs.createVariable("two_year_return_q", "f8", ("num_usgs_reaches",), fill_value=self.FLOAT_FILL)
        tyr.long_name = "USGS two_Year_Return"
        tyr.comment = "USGS two year return interval discharge value in this cell"
        tyr.units = "m^3/s"
        tyr[:] = np.nan_to_num(self.map_dict[continent]["tyr"], copy=True, nan=self.FLOAT_FILL)

        usgs.createDimension("nchars", 16)
        usgs_id = usgs.createVariable("usgs_id", "S1", ("num_usgs_reaches", "nchars"), fill_value=self.INT_FILL)
        usgs_id.long_name = "USGS_ID_number"
        usgs_id[:] = stringtochar(self.map_dict[continent]["usgs_id"].astype("S16"))

        usgs_q = usgs.createVariable("usgs_q", "f8", ("num_usgs_reaches", "num_days"), fill_value=self.FLOAT_FILL)
        usgs_q.long_name = "USGS_discharge_time_series_(daily)"
        usgs_q.comment = "Direct port from USGS"
        usgs_q.units = "m^3/s"
        usgs_q[:] = np.nan_to_num(self.map_dict[continent]["usgs_q"], copy=True, nan=self.FLOAT_FILL)

        usgs_qt = usgs.createVariable("usgs_qt", "f8", ("num_usgs_reaches", "num_days"), fill_value=self.FLOAT_FILL)
        usgs_qt.long_name = "USGS_discharge_time_series_(daily)"
        usgs_qt.comment = "Direct port from USGS"
        usgs_qt.units = "days since Jan 1 Year 1"
        usgs_qt[:] = np.nan_to_num(self.map_dict[continent]["usgs_qt"], copy=True, nan=self.FLOAT_FILL)

    def __copy_past_results(self, temp, sos):
        """Copy results of past Confluence run.
        
        Parameters
        ----------
        temp: Dataset or Group
            temporary NetCDF4 Dataset 
        sos: Dataset
            new NetCDF4 Dataset
        """
        
        self.__append_geobam(temp, sos)
        sos.createGroup("momma")
        self.__copy_sos(temp["momma"], sos["momma"])
        sos.createGroup("hivdi")
        self.__copy_sos(temp["hivdi"], sos["hivdi"])
        sos.createGroup("metroman")
        self.__copy_sos(temp["metroman"], sos["metroman"])
        self.__append_moi(temp, sos)
        self.__append_pd(temp, sos)
        sos.createGroup("offline")
        self.__copy_sos(temp["offline"], sos["offline"])

    def __copy_sos(self, temp, sos):
        """Copy SoS data to new SoS file.
        
        Parameters
        ----------
        temp: Dataset or Group
            temporary NetCDF4 Dataset 
        sos: Dataset
            new NetCDF4 Dataset
        """

        # Global attributes
        sos.setncatts(temp.__dict__)

        # Dimensions
        for name, dimension in temp.dimensions.items():
            sos.createDimension(name, dimension.size if not dimension.isunlimited() else None)

        # Variables
        for name, variable in temp.variables.items():
            v = sos.createVariable(name, variable.datatype, variable.dimensions)
            v.setncatts(temp[name].__dict__)
            v[:] = temp[name][:]               

    def map_data(self):
        """Maps USGS data to SoS organized by continent.
        
        Stores mapped data in map_dict attribute.
        """

        for continent, sos_data in self.sos_dict.items():
            if sos_data:
                # Reach identifiers
                sos_ids = sos_data["reach_id"]
                usgs_ids = self.usgs_dict["reachId"]
                same_ids = np.intersect1d(sos_ids, usgs_ids)
                indexes = np.where(np.isin(usgs_ids, same_ids))[0]

                if indexes.size == 0:
                    self.map_dict[continent] = None
                else:
                    # Map USGS data that matches SoS reach identifiers
                    self.map_dict[continent]["days"] = np.array(range(1, len(self.usgs_dict["Qwrite"][0]) + 1))
                    self.map_dict[continent]["usgs_reach_id"] = self.usgs_dict["reachId"].astype(np.int64)[indexes]
                    self.map_dict[continent]["fdq"] = self.usgs_dict["FDQS"][indexes,:]
                    self.map_dict[continent]["max_q"] =self.usgs_dict["Qmax"][indexes]
                    self.map_dict[continent]["monthly_q"] = self.usgs_dict["MONQ"][indexes,:]
                    self.map_dict[continent]["mean_q"] = self.usgs_dict["Qmean"][indexes]
                    self.map_dict[continent]["min_q"] = self.usgs_dict["Qmin"][indexes]
                    self.map_dict[continent]["tyr"] = self.usgs_dict["TwoYr"][indexes]
                    self.map_dict[continent]["usgs_id"] = np.array(self.usgs_dict["dataUSGS"])[indexes]
                    self.map_dict[continent]["usgs_q"] = self.usgs_dict["Qwrite"][indexes,:]
                    self.map_dict[continent]["usgs_qt"] = self.usgs_dict["Twrite"][indexes,:]
            else:
                self.map_dict[continent] = None

    def mv_sos(self):
        """Move SoS file to temporary directory.
    
        Returns reference to temporary directory as a pathlib.Path object.
        """

        temp_dir = tempfile.TemporaryDirectory()

        with scandir(self.sos_dir) as entries:
            for sos_file in entries:
                move(sos_file, Path(temp_dir.name) / sos_file.name)

        return temp_dir
    
    def read_sos(self):
        """Reads in data from the SoS and stores in sos_dict attribute."""

        with scandir(self.sos_dir) as entries:
            for sos_file in entries:
                continent = sos_file.name.split('_')[0]
                sos = Dataset(Path(sos_file))
                self.sos_dict[continent] = { 
                    "reach_id": sos["reaches"]["reach_id"][:].filled(np.nan).astype(int)
                }
                sos.close()