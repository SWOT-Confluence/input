# Standard imports
import os
import re
from pathlib import Path
import unittest

# Third-party imports
import numpy as np
from numpy.testing import assert_array_equal, assert_array_almost_equal

# Local imports
from input.extract.ExtractRiver import ExtractRiver, calculate_d_x_a
from input.extract.ExtractLake import ExtractLake

class TestExtract(unittest.TestCase):
    """Tests methods and functions from Extract module."""
    
    RIVER_PARENT = Path(__file__).parent / "test_data" / "river"
    RIVER_CYCLE_PASS = { "1_51": 1, "1_372": 2, "2_51": 3, "2_372": 4, "3_51": 5 }
    REACH_ID = "74269900011"
    NODE_LIST = ["74269900010011", "74269900010021", "74269900010031", "74269900010041", "74269900010051"]
    LAKE_PARENT = Path(__file__).parent / "test_data" / "lake"
    LAKE_CYCLE_PASS = { "1_1001": 1, "1_1008": 2, "1_1015": 3, "1_1022": 4, "1_1029": 5, "1_1105": 6, "1_1112": 7, "1_1119": 8, "1_1126": 9, "1_1203": 10 }
    LAKE_ID = "7720003433"
    
    def test_calculate_d_x_a(self):
        """Tests calculate_d_x_a function."""

        wse = np.array([103, 102, 101, 102, 104], dtype=np.float64)
        width = np.array([620, 713, 628, 631, 615], dtype=np.float64)
        d_x_area = calculate_d_x_a(wse, width)
        expected = np.array([620, 0, -628, 0, 1230], dtype=np.float64)
        assert_array_almost_equal(expected, d_x_area)

    def test_append_node(self):
        """Tests append_node method."""
        
        # Create ExtractRiver object
        shapefiles = self.get_shapefiles(self.RIVER_PARENT)
        ext = ExtractRiver(self.REACH_ID, shapefiles, self.RIVER_CYCLE_PASS, None, self.NODE_LIST)
        
        # Set and append reach data to node level data
        ext.data["reach"]["slope2"] = [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05]
        ext.data["node"] = {}
        ext.data["node"]["slope2"] = np.full((5, 10), np.nan, dtype=np.float64)
        ext.append_node("slope2", len(self.NODE_LIST))
        
        # Assert results
        self.assertEqual((5,10), ext.data["node"]["slope2"].shape)
        expected = [[4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05],
                    [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05],
                    [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05],
                    [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05],
                    [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05]]
        assert_array_almost_equal(expected, ext.data["node"]["slope2"])
      
    def test_extract_river(self):
        """Tests extract method for river data."""
        
        # Create ExtractRiver object
        shapefiles = self.get_shapefiles(self.RIVER_PARENT)
        ext = ExtractRiver(self.REACH_ID, shapefiles, self.RIVER_CYCLE_PASS, None, self.NODE_LIST)
        ext.extract()
        
        expected = [ 277.921069, 276.321367, 277.952135, 282.09515, 280.082443 ]
        assert_array_almost_equal(expected, ext.data["reach"]["width"])
        
        expected = [ 0.000037, 0.000037, 0.00003, 0.00003, 0.000037 ]
        assert_array_almost_equal(expected, ext.data["reach"]["slope2"])
        
        expected = [ 216.451228, 216.612945, 216.533625, 216.850681, 216.611095 ]
        assert_array_almost_equal(expected, ext.data["reach"]["wse"])
        
        expected = [ 216.451228, 216.612945, 216.533625, 216.850681, 216.611095 ]
        assert_array_almost_equal(expected, ext.data["reach"]["wse"])
        
        expected = [ [ 336.165592, 306.113385, 313.877105, 333.225042, 331.756057 ],
          [ 309.483523, 300.910719, 316.416494, 283.292237, 279.096203 ],
          [ 283.344225, 222.794758, 261.232937, 236.869731, 261.116132 ],
          [ 243.154441, 247.801235, 254.777402, 256.026360, 231.419231 ],
          [ 227.331508, 221.261808, 219.958754, 189.492747, 251.536026 ] ]
        assert_array_almost_equal(expected, ext.data["node"]["width"])
        
        expected = [ [ 0.000037, 0.000037, 0.00003, 0.00003, 0.000037 ],
           [ 0.000037, 0.000037, 0.00003, 0.00003, 0.000037 ],
           [ 0.000037, 0.000037, 0.00003, 0.00003, 0.000037 ],
           [ 0.000037, 0.000037, 0.00003, 0.00003, 0.000037 ],
           [ 0.000037, 0.000037, 0.00003, 0.00003, 0.000037 ] ]
        assert_array_almost_equal(expected, ext.data["node"]["slope2"])
 
        expected = [ [ 216.276747, 216.361219, 216.603850, 216.746326, 216.716955 ],
                [ 216.270577, 216.480211, 216.384466, 216.898126, 216.650168 ],
                [ 216.658787, 216.530280, 216.661911, 216.575435, 216.557089 ],
                [ 216.333707, 216.684610, 216.572820, 216.748619, 216.613460 ],
                [ 216.322851, 216.415144, 216.400287, 216.758574, 216.507374 ] ]
        assert_array_almost_equal(expected, ext.data["node"]["wse"])
        
    def test_extract_lake(self):
        """Tests extract method for lake data."""
        
        # Create ExtractRiver object
        shapefiles = self.get_shapefiles(self.LAKE_PARENT)
        lake = ExtractLake(self.LAKE_ID, shapefiles, self.LAKE_CYCLE_PASS, None)
        lake.extract()
        
        expected = [ -2.397356e-07, 6.596707e-08, -1.114663e-07, 5.921538e-08, 1.399049e-07, 9.170604e-08, -1.312332e-07, 2.863243e-07, -4.733622e-08, -5.238752e-08 ]
        assert_array_almost_equal(expected, lake.data["delta_s_q"])
        
        expected = [ "2008-10-01", "2008-10-08", "2008-10-15", "2008-10-22", "2008-10-29", "2008-11-05", "2008-11-12", "2008-11-19", "2008-11-26", "2008-12-03" ]
        assert_array_equal(expected, lake.data["time_str"])
        
    def get_shapefiles(self, sdir):
        with os.scandir(sdir) as entries:
            shpfiles = [str(Path(entry)) for entry in entries]
        shpfiles.sort(key=self.sort_shapefiles)
        return shpfiles

    def sort_shapefiles(self, shapefile):
        """Sort shapefiles so that they are in ascending order."""
        
        return [ self.strtoi(shp) for shp in re.split(r'(\d+)', shapefile) ]

    def strtoi(self, text):
        return int(text) if text.isdigit() else text