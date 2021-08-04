# Third party imports
import rpy2.robjects as robjects
from rpy2.robjects import numpy2ri
from rpy2.robjects.packages import importr

# Print R warnings
robjects.r['options'](warn=1)

class Kluge:
    """Class that removes and tracks invalid nodes and time steps.

    Operations take SWOT NetCDF files and remove invalid node and time and
    track invalid locations in a JSON file.
    
    Attributes
    ----------
    invalid_json: Path
        Path to invalid JSON file
    KLUGE: rpy2.robjects.packages.InstalledSTPackage
        KLUGE object to perform kluge operations
    out_dir: Path
        Path to directory where NetCDF files should be written to.
    swot_dir: Path
        Path to directory of SWOT files
    Methods
    -------
    kluge_data()
        Tracks and removes invalid data
    """

    KLUGE = importr("Kluge")

    def __init__(self, swot_dir, out_dir, invalid_json):
        """
        Parameters
        ----------
        invalid_json: Path
            Path to invalid JSON file
        out_dir: Path
            Path to directory where NetCDF files should be written to.
        swot_dir: Path
            Path to directory of SWOT files
        """

        self.swot_dir = swot_dir
        self.out_dir = out_dir
        self.invalid_json = invalid_json

    def kluge_data(self):
        """Removes and tracks invalid data."""
        
        self.KLUGE.main(in_dir = str(self.swot_dir), out_dir = str(self.out_dir), 
            invalid_file = str(self.invalid_json))