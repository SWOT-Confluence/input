# Third-party imports
from netCDF4 import stringtochar
import numpy as np

# Local imports
from input.write.WriteStrategy import WriteStrategy

class WriteLake(WriteStrategy):
    """A class that extends WriteStrategy to write river data to NetCDF.
    
    Intermediate data is stored in NetCDF file format and is taken from various
    AWS S3 buckets.

    Attributes
    ----------
    node_ids: list
        list of string node identifiers

    Methods
    -------
    create_dimensions(dataset)
        create dimensions and coordinate variables for dataset
    write_data(dataset, data)
        writes SWOT data dictionaries to NetCDF files organized by continent
    """
    
    def __init__(self, swot_id, output_dir):
        """
        Parameters
        ----------
        swot_id: int
           unique SWOT identifier
        output_dir: Path
            path to output directory on EFS 'input' mount
        """
        
        super().__init__(swot_id, output_dir)
    
    def create_dimensions(self, dataset, obs_times):
        """Create dimensions and coordinate variables for dataset.
        
        Parameters
        ----------
        dataset: netCDF4.Dataset
            dataset to write node level data to
        obs_times: list
            list of string cycle/pass identifiers
        """

         # Create dimension and coordinate variable
        dataset.createDimension("nt", len(obs_times))
        nt_v = dataset.createVariable("nt", "i4", ("nt",))
        nt_v.units = "pass"
        nt_v.long_name = "time steps"
        nt_v[:] = range(len(obs_times))
        
    def write_data(self, dataset, data):
        """Writes lake SWOT data to NetCDF format.
        
        TODO:
        - Figure out maximum cycle/pass length.
        
        Parameters
        ----------
        dataset: netCDF4.Dataset
            netCDF4 dataset to set global attirbutes for
        data: dict
            dictionary of SWOT data variables
        """
        
        lake_id_v = dataset.createVariable("lake_id", "S1", ("nchars",),
                                           fill_value=self.STR_FILL)
        lake_id_v.long_name = "lake ID(s) from prior database"
        lake_id_v.comment = "List of identifiers of prior lakes that " \
            + "intersect the observed lake. The format of the identifier " \
            + "is CBBNNNNNNT, where C=continent code, B=basin code, N=lake " \
            + "counter within the basin, T=type. The different lake " \
            + "identifiers are separated by semicolons."        
        lake_id_v[:] = stringtochar(np.array(self.swot_id, dtype="S10"))
        
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
        time_str[:] = stringtochar(data["time_str"].astype("S20"))
        
        delta_s_q = dataset.createVariable("delta_s_q", "f8", ("nt",), 
                                           fill_value=self.FLOAT_FILL)
        delta_s_q.long_name = "storage change computed by quadratic method"
        delta_s_q.units = "km^3"
        delta_s_q.valid_min = -1000
        delta_s_q.valid_max = 1000
        delta_s_q.comment = "Storage change with regards to the reference " \
            + "area and height from PLD; computed by the quadratic method."
        delta_s_q[:] = data["delta_s_q"]
        
        
        