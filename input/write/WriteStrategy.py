# Standard imports
from abc import abstractmethod
from datetime import datetime

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np

class WriteStrategy:
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
    output_dir: Path
        path to output directory on EFS 'input' mount
    swot_id: int
           unique SWOT identifier

    Methods
    -------
    create_dimensions(dataset)
        create dimensions and coordinate variables for dataset
    define_global_attrs(dataset)
        Set global attributes for NetCDF dataset file
    define_global_obs(dataset, obs_times)
        define global observation NetCDF variable
    write(data, obs_times)
        executes write operations
    write_data(dataset, data)
        writes SWOT data dictionaries to NetCDF files organized by continent
    """

    CONTINENTS = { 1: "AF", 2: "EU", 3: "AS", 4: "AS", 5: "OC", 6: "SA", 7: "NA", 8: "NA", 9:"NA" }
    FLOAT_FILL = -999999999999
    INT_FILL = -999

    def __init__(self, swot_id, output_dir):
        """
        Parameters
        ----------
        swot_id: int
           unique SWOT identifier
        output_dir: Path
            path to output directory on EFS 'input' mount
        """
        
        self.output_dir = output_dir
        self.swot_id = swot_id
        
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'create_dimensions') and 
                callable(subclass.create_dimensions) and 
                hasattr(subclass, 'write_data') and 
                callable(subclass.write_data) or
                NotImplemented)

    @abstractmethod
    def create_dimensions(self, dataset):
        """Create dimensions and coordinate variables for dataset.
        
        Parameters
        ----------
        dataset: netCDF4.Dataset
            dataset to write node level data to
        obs_times: list
            list of string cycle/pass identifiers
        """

        raise NotImplementedError
        
    def define_global_attrs(self, dataset):
        """Set global attributes for NetCDF dataset file.

        Currently sets title, history, and continent.

        Parameters
        ----------
        dataset: netCDF4.Dataset
            netCDF4 dataset to set global attirbutes for
        """

        dataset.title = f"SWOT Data for {self.swot_id}"
        dataset.reach_id = int(self.swot_id)
        dataset.history = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
        dataset.continent = self.CONTINENTS[int(str(self.swot_id)[0])]
    
    def define_global_obs(self, dataset, obs_times):
        """Define global observation NetCDF variable.
        
        Parameters
        ----------
        dataset: netCDF4.Dataset
            netCDF4 dataset to set global attirbutes for
        obs_times: list
            list of string cycle/pass identifiers
        """
        
        dataset.createDimension('nchars', 10)
        obs = dataset.createVariable("observations", "S1", ("nt", "nchars"))
        obs.units = "pass"
        obs.long_name = "cycle/pass observations"
        obs.comment = "A list of cycle and pass numeric identifiers that " \
            + "identify each reach and node observation. An array element " \
            + "is comprised of 'cycle/pass' as a string value."
        obs[:] = stringtochar(np.array(obs_times, dtype="S10"))
    
    @abstractmethod
    def write_data(self, dataset, data):
        """Writes SWOT data to NetCDF format.
        
        Parameters
        ----------
        dataset: netCDF4.Dataset
            netCDF4 dataset to set global attirbutes for
        data: dict
            dictionary of SWOT data variables
        """
        
        raise NotImplementedError

    def write(self, data, obs_times):
        """Writes node and reach level SWOT data to NetCDF format.
        
        TODO:
        - Figure out maximum cycle/pass length.
        
        Parameters
        ----------
        data: dict
            dictionary of SWOT data variables
        obs_times: list
            list of string cycle/pass identifiers
        """

        # NetCDF4 dataset
        swot_file = self.output_dir / "swot" / f"{self.swot_id}_SWOT.nc"
        dataset = Dataset(swot_file, 'w', format="NETCDF4")
        self.define_global_attrs(dataset)

        # Dimension and data
        self.create_dimensions(dataset, obs_times)
        
        # Global observation variable
        self.define_global_obs(dataset, obs_times)

        # Reach and node data
        self.write_data(dataset, data)

        dataset.close()