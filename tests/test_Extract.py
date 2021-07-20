# Standard imports
import json
from pathlib import Path
import unittest

# Third-party imports
import geopandas as gpd
import numpy as np

# Local imports
from src.Extract import Extract, calculate_d_x_a, extract_node_local, extract_reach_local

class TestExtract(unittest.TestCase):
    """Tests methods and functions from Extract module."""

    def test_append_node(self):
        """Tests append_node method."""

        reach_dict = {}
        reach_dict["na"] = {"reach_id": np.array([77449100061, 77449100062]),
                            "slope2": np.reshape(np.array([0.0001, 0.000095, 0.000098, 0.00009]), (2, 2))}
        node_dict = {}
        node_dict["na"] = {"nt": 2,
                           "nx": 10,
                           "reach_id": np.array([77449100061, 77449100061, 77449100061, 77449100061, 77449100061,
                                                 77449100062, 77449100062, 77449100062, 77449100062, 77449100062])}
        ext = Extract()
        ext.reach_data = reach_dict
        ext.node_data = node_dict
        ext.append_node("slope2")
        
        self.assertEqual((2,10), ext.node_data["na"]["slope2"].shape)

        expected_col = np.array([0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 
                                 0.000095, 0.000095, 0.000095, 0.000095, 0.000095])
        np.testing.assert_almost_equal(expected_col, ext.node_data["na"]["slope2"][0,:])
        
        expected_node = np.vstack((np.array([0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 
                                 0.000095, 0.000095, 0.000095, 0.000095, 0.000095]),
                                 np.array([0.000098, 0.000098, 0.000098, 0.000098, 0.000098, 
                                 0.00009, 0.00009, 0.00009, 0.00009, 0.00009])))
        np.testing.assert_almost_equal(expected_node, ext.node_data["na"]["slope2"])     
        
    def test_calculate_d_x_a(self):
        """Tests calculate_d_x_a function."""

        shape_dir = Path(__file__).parent / "test_data"
        time_list = [109, 130, 220, 313, 403, 424, 515, 605, 626]

        width = np.array([])
        wse = np.array([])
        for time in time_list:
            reach_sf = shape_dir / str(time) / "riverobs_nominal_20201105" / "river_data" / "reaches.shp"
            reach_df = gpd.read_file(reach_sf)
            width = np.append(width, reach_df["width"].to_numpy())
            wse = np.append(wse, reach_df["wse"].to_numpy())

        dA = calculate_d_x_a(wse, width)

        expected = np.array([-42.901832538, -44.663501891, 834.819106112, 27.642266635, 
            -26.825309207, -17.3424814, 13.22063419, 0, 34.112150982])
        np.testing.assert_almost_equal(expected, dA, 2)

    def test_extract_node_local(self):
        """Tests extract_node_local function."""

        node_path = Path(__file__).parent / "test_data" / "109"/ "riverobs_nominal_20201105" / "river_data" / "nodes.shp"
        time = 0
        node_dict = {}
        with open(Path(__file__).parent.parent / "src" / "data" / "sac.json") as f:
            sac_node = json.load(f)["sac_nodes"]
        extract_node_local(node_path, time, node_dict, sac_node)
        
        expected_reach = np.array([77449100061, 77449100061, 77449100061, 77449100061, 77449100061])
        np.testing.assert_array_equal(expected_reach,node_dict["reach_id"][:5])

        expected_node = np.array([77449100060011, 77449100060021, 77449100060031, 77449100060041, 77449100060051])
        np.testing.assert_array_equal(expected_node, node_dict["node_id"][:5])

        expected_width = np.array([43.208299, 69.361156, 76.104040, 92.461620, 76.389673])
        np.testing.assert_array_almost_equal(expected_width, node_dict["width"][:5])
        
        expected_wse = np.array([7.64898, 7.42904, 7.48903, 7.49127, 7.71399])
        np.testing.assert_array_almost_equal(expected_wse, node_dict["wse"][:5])

        expected_nan = np.full((2154), fill_value=np.nan)
        np.testing.assert_array_almost_equal(expected_nan, node_dict["width"][5:])
        np.testing.assert_array_almost_equal(expected_nan, node_dict["wse"][5:])

        node_path = Path(__file__).parent / "test_data" / "130"/ "riverobs_nominal_20201105" / "river_data" / "nodes.shp"
        time = 1
        extract_node_local(node_path, time, node_dict, sac_node)
        expected_width = np.vstack((np.array([43.208299, 69.361156, 76.104040, 92.461620, 76.389673]),
            np.array([62.901061, 58.018872, 63.814723, 76.794963, 90.156724])))
        np.testing.assert_array_almost_equal(expected_width, node_dict["width"][:2,:5])
        expected_wse = np.vstack((np.array([7.64898, 7.42904, 7.48903, 7.49127, 7.71399]), 
            np.array([7.65856, 7.56269, 7.48027, 7.78397, 7.66650])))
        np.testing.assert_array_almost_equal(expected_wse, node_dict["wse"][:2,:5])

    def test_extract_reach_local(self):
        """Tests extract_reach_local funtion."""

        reach_path = Path(__file__).parent / "test_data" / "109"/ "riverobs_nominal_20201105" / "river_data" / "reaches.shp"
        time = 0
        reach_dict = {}
        with open(Path(__file__).parent.parent / "src" / "data" / "sac.json") as f:
            sac_reach = json.load(f)["sac_reaches"]
        extract_reach_local(reach_path, time, reach_dict, sac_reach)
        
        np.testing.assert_array_almost_equal(np.array([77449100061]), reach_dict["reach_id"][0])
        np.testing.assert_array_almost_equal(np.array([79.981045]), reach_dict["width"][0])
        np.testing.assert_array_almost_equal(np.array([7.99663]), reach_dict["wse"][0])
        np.testing.assert_array_almost_equal(np.array([0.0001]), reach_dict["slope2"][0])

        expected_nan = np.full((40), fill_value=np.nan)
        np.testing.assert_array_almost_equal(expected_nan, reach_dict["width"][1:])
        np.testing.assert_array_almost_equal(expected_nan, reach_dict["wse"][1:])
        np.testing.assert_array_almost_equal(expected_nan, reach_dict["slope2"][1:])

        reach_path = Path(__file__).parent / "test_data" / "130"/ "riverobs_nominal_20201105" / "river_data" / "reaches.shp"
        time = 1
        extract_reach_local(reach_path, time, reach_dict, sac_reach)
        expected_width = np.vstack((np.array([79.981045]), np.array([102.228203])))
        np.testing.assert_array_almost_equal(expected_width, reach_dict["width"][:2, :1])
        expected_wse = np.vstack((np.array([7.99663]), np.array([8.09615])))
        np.testing.assert_array_almost_equal(expected_wse, reach_dict["wse"][:2, :1])
        expected_slope2 = np.vstack((np.array([0.0001]), np.array([0.000095])))
        np.testing.assert_array_almost_equal(expected_slope2, reach_dict["slope2"][:2, :1])