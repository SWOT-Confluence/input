"""ExtractStrategy module 

Class
-----
ExtractStrategy
    Abstract parent class that that extracts and concatenates SWOT observations 
    from shapefiles.

Functions
---------
extract_passes(continent)
    Retrieve pass and cycle identifiers for continent from shapefiles.
"""

# Standard imports
from abc import ABCMeta, abstractmethod
import glob
from pathlib import Path

# Constants
CONT_LOOKUP = { 1: "AF", 2: "EU", 3: "AS", 4: "AS", 5: "OC", 6: "SA", 
                    7: "NA", 8: "NA", 9: "NA"}

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
    """
    
    LOCAL_INPUT = Path("/mnt/data/shapefiles/swot")    # local
    
    def __init__(self, confluence_fs, swot_id):
        """
        Parameters
        ----------
        confluence_fs: S3FileSystem
            references Confluence S3 buckets
        swot_id: int
            unique SWOT identifier (identifies continent)
        """
        
        self.confluence_fs = confluence_fs        
        self.cycle_data = extract_passes(int(str(swot_id)[0]), self.confluence_fs)
        # self.cycle_data = extract_passes_local(int(str(swot_id)[0]), self.LOCAL_INPUT)    # local
        self.obs_times = []
    
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'extract_data') and 
                callable(subclass.extract_data) and 
                hasattr(subclass, 'extract_data_local') and 
                callable(subclass.extract_data_local) or
                NotImplemented)

    @abstractmethod
    def extract(self):
        """Extracts data from confluence_fs S3 bucket and stores in data dict."""

        raise NotImplementedError
    
    @abstractmethod
    def extract_local(self):
        """Extracts data from SWOT shapefiles and stores in data dictionaries."""
        
        raise NotImplementedError
    
    
def extract_passes(c_id, confluence_fs):
    """Retrieve pass and cycle identifiers for continent from shapefiles.
    
    Parameters
    ----------
    c_id: int
        Continent integer identifier
    confluence_fs: S3FileSystem
        references Confluence S3 buckets (holds shapefiles)
    
    Returns
    -------
    dictionary of cycle keys and pass values
    """
    
    # Locate cycle/pass identifiers for continent
    c_abr = CONT_LOOKUP[c_id]
    c_files = [Path(c_file).name for c_file in confluence_fs.glob(f"confluence-swot/*reach*{c_abr}*.shp")]
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
    
def extract_passes_local(c_id, swot_dir):
    """Retrieve pass and cycle identifiers for continent from shapefiles.
    
    Parameters
    ----------
    c_id: int
        Continent integer identifier
    swot_dir: Path
        Path to local SWOT directory that contains shapefiles
    
    Returns
    -------
    dictionary of cycle keys and pass values
    """
    
    # Locate cycle/pass identifiers for continent
    c_abr = CONT_LOOKUP[c_id]
    c_files = [Path(c_file).name for c_file in glob.glob(str(swot_dir / f"*reach*{c_abr}*.shp"))]
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