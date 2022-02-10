# Standard imports
from abc import ABCMeta, abstractmethod
import json

# Class
class ExtractStrategy(metaclass=ABCMeta):
    """A class that extracts and concatenates SWOT observations from shapefiles.
    
    Time series data is concatenated over shapefiles.

    Attributes
    ----------
    confluence_fs: S3FileSystem
        references Confluence S3 buckets
    cycle_data: dict
        dictionary of cycle identifier keys with pass identifier values
    obs_times: list
        list of 'cycle/pass' values that contained observations for this reach

    Methods
    -------
    extract(confluence_fs)
        extracts data from S3 bucket shapefiles and stores in data dictionaries
    extract_passes(continent)
        retrieve pass and cycle identifiers for continent from shapefiles.
    """
    
    # LOCAL_INPUT = Path("/mnt/data/shapefiles/swot/river")    # local
    CONT_LOOKUP = { 1: "AF", 2: "EU", 3: "AS", 4: "AS", 5: "OC", 6: "SA", 
                    7: "NA", 8: "NA", 9: "NA"}
    
    def __init__(self, confluence_fs, swot_id, cycle_pass_json):
        """
        Parameters
        ----------
        confluence_fs: S3FileSystem
            references Confluence S3 buckets
        swot_id: int
            unique SWOT identifier (identifies continent)
        cycle_pass_json: Path
            path to cycle pass JSON file
        """
        
        self.confluence_fs = confluence_fs        
        self.cycle_data = self.extract_passes(int(str(swot_id)[0]))
        # self.cycle_data = self.extract_passes_local(int(str(swot_id)[0]))    # local
        self.obs_times = []
        with open(cycle_pass_json, 'r') as jf:
            self.pass_data = json.load(jf)
    
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'extract_data') and 
                callable(subclass.extract_data) and 
                hasattr(subclass, 'extract_data_local') and 
                callable(subclass.extract_data_local) and
                hasattr(subclass, 'retrieve_swot_files') and 
                callable(subclass.retrieve_swot_files) and 
                hasattr(subclass, 'retrieve_swot_files_local') and 
                callable(subclass.retrieve_swot_files_local) or
                NotImplemented)

    @abstractmethod
    def extract(self):
        """Extracts data from confluence_fs S3 bucket and stores in data dict."""

        raise NotImplementedError
    
    @abstractmethod
    def extract_local(self):
        """Extracts data from SWOT shapefiles and stores in data dictionaries."""
        
        raise NotImplementedError    
    
    def extract_passes(self, c_id):
        """Retrieve pass and cycle identifiers for continent from shapefiles.
        
        Parameters
        ----------
        c_id: int
            Continent integer identifier
        
        Returns
        -------
        dictionary of cycle keys and pass values
        """
        
        # Locate cycle/pass identifiers for continent
        c_files = self.retrieve_swot_files(c_id)
        c_dict = {}
        for c_file in c_files:
            key = int(c_file.split('_')[5])
            if key in c_dict.keys():
                c_dict[key].append(int(c_file.split('_')[6]))
            else:
                c_dict[key] = [int(c_file.split('_')[6])]
        
        # Sort pass identifiers for each cycle
        for value in c_dict.values(): value.sort()
        
        return c_dict
        
    def extract_passes_local(self, c_id):
        """Retrieve pass and cycle identifiers for continent from shapefiles.
        
        Parameters
        ----------
        c_id: int
            Continent integer identifier
        
        Returns
        -------
        dictionary of cycle keys and pass values
        """
        
        # Locate cycle/pass identifiers for continent
        c_files = self.retrieve_swot_files_local(c_id)
        c_dict = {}
        for c_file in c_files:
            key = int(c_file.split('_')[5])
            if key in c_dict.keys():
                c_dict[key].append(int(c_file.split('_')[6]))
            else:
                c_dict[key] = [int(c_file.split('_')[6])]
        
        # Sort pass identifiers for each cycle
        for value in c_dict.values(): value.sort()
        
        return c_dict
    
    @abstractmethod
    def retrieve_swot_files(self, c_id):
        """Retrieve SWOT Lake shapefiles.
        
        Parameters
        ----------
        c_id: int
            Continent integer identifier
        """
        
        raise NotImplementedError
    
    @abstractmethod
    def retrieve_swot_files_local(self, c_id):
        """Retrieve SWOT Lake shapefiles.
        
        Parameters
        ----------
        c_id: int
            Continent integer identifier
        """
        
        raise NotImplementedError