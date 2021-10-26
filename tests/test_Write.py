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
from input.Write import Write

class TestWrite(unittest.TestCase):
    """Tests methods from Write class."""

    def test_write_data(self):
        """Tests write_data method."""

        rf = Path(__file__).parent / "test_data" / "write_reach_data"
        with open(rf, "rb") as pf:
            reach_data = pickle.load(pf)

        nf = Path(__file__).parent / "test_data" / "write_node_data"
        with open(nf, "rb") as pf:
            node_data = pickle.load(pf)

        output_dir = Path(__file__).parent / "write"
        if not output_dir.exists(): (output_dir / "swot").mkdir(parents=True, exist_ok=True)
        
        write = Write(node_data, reach_data, output_dir)
        write.write_data()
        
        with scandir(Path(output_dir / "swot")) as files:
            count = len([file for file in files])
        self.assertEqual(41, count)

        swot_file = Path(output_dir) / "swot" / "77449100061_SWOT.nc"
        swot = Dataset(swot_file)
        self.assertEqual(77449100061, swot.reach_id)
        self.assertEqual(49, len(swot["node"]["node_id"]))
        
        # Reach
        expected_width = np.array([79.981045, 102.228203, 131.835053, 72.060132, 73.473868, 73.547419, 74.904443, 73.93421, 87.579335])
        np.testing.assert_array_almost_equal(expected_width, swot["reach"]["width"][:].filled(np.nan))

        expected_width_u = np.array([0.939375, 1.545424, 0.967832, 0.683558, 0.666685, 0.698807, 0.735233, 0.745103, 0.950822])
        np.testing.assert_array_almost_equal(expected_width_u, swot["reach"]["width_u"][:].filled(np.nan))
        
        expected_wse = np.array([7.99663, 8.09615, 14.86537, 8.91665, 8.16795, 8.29723, 8.7095, 8.53303, 8.92249])
        np.testing.assert_array_almost_equal(expected_wse, swot["reach"]["wse"][:].filled(np.nan))
        
        expected_slope = np.array([0.00010045794, 9.540541e-05, 9.765124e-05, 8.985157e-05, 0.00010460104, 0.00013338136, 0.00011058383, 1.900279e-05, 8.804341e-05])
        np.testing.assert_array_almost_equal(expected_slope, swot["reach"]["slope2"][:].filled(np.nan))

        expected_slope_u = np.array([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
        np.testing.assert_array_almost_equal(expected_slope_u, swot["reach"]["slope2_u"][:].filled(np.nan))

        expected_dxa_u = np.array([141508411.405, 141511250.586, 141609416.134, 141520995.491, 141510481.11, 141512313.117, 141518213.954, 141515667.59, 141521953.203])
        np.testing.assert_array_almost_equal(expected_dxa_u, swot["reach"]["d_x_area_u"][:].filled(np.nan))

        # Node
        expected_width = np.array([43.208299, 62.901061, 85.893091, 44.241324, 49.556449, 30.668317, 60.893438, 50.442531, 44.137341])
        np.testing.assert_array_almost_equal(expected_width, swot["node"]["width"][:].filled(np.nan)[0,:])
        
        expected_width_u = np.array([4.635789, 5.6747802, 4.6544526, 3.5315369, 3.5679297, 2.4884082, 5.7413614, 3.8802125, 4.4176386])
        np.testing.assert_array_almost_equal(expected_width_u, swot["node"]["width_u"][:].filled(np.nan)[0,:])

        expected_wse = np.array([7.64898, 7.65856, 14.55173, 8.47558, 7.55344, 8.10743, 8.22878, 7.98136, 8.5523])
        np.testing.assert_array_almost_equal(expected_wse, swot["node"]["wse"][:].filled(np.nan)[0,:])
        
        np.testing.assert_array_almost_equal(expected_slope, swot["node"]["slope2"][0,:].filled(np.nan))

        np.testing.assert_array_almost_equal(expected_slope_u, swot["node"]["slope2_u"][0,:].filled(np.nan))

        np.testing.assert_array_almost_equal(expected_dxa_u, swot["node"]["d_x_area_u"][0,:].filled(np.nan))

        # Remove written files
        swot.close()
        rmtree(output_dir)