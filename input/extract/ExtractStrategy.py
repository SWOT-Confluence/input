# Standard imports
from abc import ABCMeta, abstractmethod
import json

# Class
class ExtractStrategy(metaclass=ABCMeta):
    """A class that extracts and concatenates SWOT observations from shapefiles.
    
    Time series data is concatenated over shapefiles.

    Attributes
    ----------
    swot_id: int
            unique SWOT identifier (identifies continent)       
    cycle_pass: list
        list of cycle passes in the form "c_p".
    shapefiles: list
        list of SWOT shapefiles

    Methods
    -------
    extract()
        extracts data from S3 bucket shapefiles and stores in data dictionaries.
    extract_local()
        extracts data from local file system and stores in data dictionaries.
    """
    
    def __init__(self, swot_id, shapefiles, cycle_pass, creds=None):
        """
        Parameters
        ----------
        swot_id: int
            unique SWOT identifier (identifies continent)       
        shapefiles: list
            list of SWOT shapefiles
        cycle_pass: dict
            dictionary of cycle pass data
        creds: dict
            dictionary of AWS S3 credentials
        """
        
        self.swot_id = swot_id
        self.shapefiles = shapefiles
        self.obs_times = []
        self.cycle_pass = cycle_pass
        self.creds = creds
    
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'extract') and 
                callable(subclass.extract) or
                NotImplemented)

    @abstractmethod
    def extract(self):
        """Extracts data from confluence_fs S3 bucket and stores in data dict."""

        raise NotImplementedError