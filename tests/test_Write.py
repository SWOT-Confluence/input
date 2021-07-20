# Standard imports
from os import scandir
from pathlib import Path
import pickle
from shutil import rmtree
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from src.Write import Write

class TestWrite(unittest.TestCase):
    """Tests methods from Write class."""

    def test_write_data(self):
        """Tests write_data method."""

        rf = Path(__file__).parent / "test_write" / "reach_data"
        with open(rf, "rb") as pf:
            reach_data = pickle.load(pf)

        nf = Path(__file__).parent / "test_write" / "node_data"
        with open(nf, "rb") as pf:
            node_data = pickle.load(pf)
        
        write = Write(node_data, reach_data)
        write.write_data()

        temp_dir = write.temp_dir.name
        with scandir(Path(temp_dir)) as files:
            count = len([file for file in files])
        self.assertEqual(41, count)

        swot_file = Path(temp_dir) / "77449100061_SWOT.nc"
        swot = Dataset(swot_file)
        self.assertEqual(77449100061, swot.reach_id)
        self.assertEqual(49, len(swot["node"]["node_id"]))
        
        # Reach
        expected_width = np.array([79.981045, 102.228203, 131.835053, 72.060132, 73.473868, 73.547419, 74.904443, 73.93421, 87.579335])
        np.testing.assert_array_almost_equal(expected_width, swot["reach"]["width"][:])
        expected_wse = np.array([7.99663, 8.09615, 14.86537, 8.91665, 8.16795, 8.29723, 8.7095, 8.53303, 8.92249])
        np.testing.assert_array_almost_equal(expected_wse, swot["reach"]["wse"][:])
        expected_slope = np.array([0.00010045794, 9.540541e-05, 9.765124e-05, 8.985157e-05, 0.00010460104, 0.00013338136, 0.00011058383, 1.900279e-05, 8.804341e-05])
        np.testing.assert_array_almost_equal(expected_slope, swot["reach"]["slope2"][:])
        expected_da = np.array([0,0,0,0,0,0,0,0,0])
        np.testing.assert_array_almost_equal(expected_da, swot["reach"]["d_x_area"])

        # Node
        expected_width = np.array([43.208299, 69.361156, 76.104040, 92.461620, 76.389673])
        np.testing.assert_array_almost_equal(expected_width, swot["node"]["width"][:5,0])
        expected_wse = np.array([7.64898, 7.42904, 7.48903, 7.49127, 7.71399])
        np.testing.assert_array_almost_equal(expected_wse, swot["node"]["wse"][:5,0])
        np.testing.assert_array_almost_equal(expected_slope, swot["node"]["slope2"][0,:])
        np.testing.assert_array_almost_equal(expected_da, swot["node"]["d_x_area"][0, :])

        # Remove written files
        swot.close()
        rmtree(temp_dir)