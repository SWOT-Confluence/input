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

        rf = Path(__file__).parent / "test_data" / "reach_data"
        with open(rf, "rb") as pf:
            reach_data = pickle.load(pf)

        nf = Path(__file__).parent / "test_data" / "node_data"
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
        expected_width = np.array([79.981045, np.nan, 76.146317, 102.228203, np.nan, 103.044978, 131.835053, np.nan, 76.542925, 72.060132, np.nan, 75.691517, 73.473868, np.nan, 86.915242, 73.547419, np.nan, 86.575635, 74.904443, np.nan, 80.18924, 73.93421, np.nan, 87.387335, 87.579335])
        np.testing.assert_array_almost_equal(expected_width, swot["reach"]["width"][:].filled(np.nan))
        expected_wse = np.array([7.99663, np.nan, 7.91816, 8.09615, np.nan, 7.92864, 14.86537, np.nan, 9.71207, 8.91665, np.nan, 8.92444, 8.16795, np.nan, 8.79917, 8.29723, np.nan, 8.88748, 8.7095, np.nan, 8.6242, 8.53303, np.nan, 8.8828, 8.92249])
        np.testing.assert_array_almost_equal(expected_wse, swot["reach"]["wse"][:].filled(np.nan))
        expected_slope = np.array([0.00010045794, np.nan, 0.00010173043, 9.540541e-05, np.nan, 0.00011160423, 9.765124e-05, np.nan, 0.00010503138, 8.985157e-05, np.nan, 9.279268e-05, 0.00010460104, np.nan, 0.00010018548, 0.00013338136, np.nan, 0.00010086814, 0.00011058383, np.nan, 9.262967e-05, 1.900279e-05, np.nan, 6.059819e-05, 8.804341e-05])
        np.testing.assert_array_almost_equal(expected_slope, swot["reach"]["slope2"][:].filled(np.nan))
        expected_dxa = np.array([-842.2044029, np.nan, -551.766112, -789.73331382, np.nan, -805.94259508, -562.88294229, np.nan, -463.77205172, -539.18129047, np.nan, -480.70395691, -577.45243603, np.nan, -642.45921666, -569.39455673, np.nan, -583.28948871, -563.43122025, np.nan, -527.68729855, -596.39991641, np.nan, -662.4912515,  -902.31105895])
        np.testing.assert_array_almost_equal(expected_dxa, swot["reach"]["d_x_area"][:].filled(np.nan))

        # Node
        expected_width = np.array([78.445637, np.nan, 52.805209, 63.560909, np.nan, 19.974453, 112.487676, np.nan, 71.697159, 40.530711, np.nan, 73.463351, 63.959729, np.nan, 75.655664, 64.405578, np.nan, 77.255957, 90.899459, np.nan, 68.158165, 81.919875, np.nan, 48.743772, 71.174781])
        np.testing.assert_array_almost_equal(expected_width, swot["node"]["width"][:].filled(np.nan)[5,:])
        expected_wse = np.array([7.69254, np.nan, 7.7044, 7.79272, np.nan, 6.77293, 14.44554, np.nan, 9.28703, 6.89793, np.nan, 8.54086, 7.86859, np.nan, 8.36375, 8.07284, np.nan, 8.32636, 8.41337, np.nan, 8.19125, 8.28667, np.nan, 8.32908, 8.61688])
        np.testing.assert_array_almost_equal(expected_wse, swot["node"]["wse"][:].filled(np.nan)[5,:])
        np.testing.assert_array_almost_equal(expected_slope, swot["node"]["slope2"][0,:].filled(np.nan))
        np.testing.assert_array_almost_equal(expected_dxa, swot["node"]["d_x_area"][0,:].filled(np.nan))

        # Remove written files
        swot.close()
        rmtree(temp_dir)