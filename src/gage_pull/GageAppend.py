# Standard imports
from datetime import datetime
import glob
from os import scandir
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np
import pandas as pd

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
    usgs_dict: dict
        Dictionary of USGS data

    Methods
    -------
    append()
        Appends USGS data to the SoS
    map_data()
        Maps USGS data to SoS data organized by continent.
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
        usgs_dict: dict
            Dictionary of USGS data
        """

        self.sos_dir = sos_dir
        self.usgs_dict = usgs_dict
        self.map_dict = { "af": {}, "as": {}, "eu": {}, "na": {},
            "oc": {}, "sa": {} }
        self.sos_dict = { "af": None, "as": None, "eu": None, "na": None,
            "oc": None, "sa": None }

    def append_data(self):
        """Appends USGS data to the SoS.
        
        Data is stored in a group labelled usgs nested under model.
        """

        for continent in self.sos_dict.keys():

            if self.map_dict[continent]:
                sos_file = glob.glob(f"{self.sos_dir}/{continent}*")[0]
                sos = Dataset(sos_file, 'a')
                sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
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
                mean_q.long_name = "USGS mean_discahrge"
                mean_q.comment = "USGS mean discharge value in this cell"
                mean_q.units = "m^3/s"
                mean_q[:] = np.nan_to_num(self.map_dict[continent]["mean_q"], copy=True, nan=self.FLOAT_FILL)

                min_q = usgs.createVariable("min_q", "f8", ("num_usgs_reaches",), fill_value=self.FLOAT_FILL)
                min_q.long_name = "USGS minimum_discahrge"
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

                sos.close()

    def map_data(self):
        """Maps USGS data to SoS organized by continent.
        
        Stores mapped data in map_dict attribute.
        """

        for continent, sos_data in self.sos_dict.items():
            
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