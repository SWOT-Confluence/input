# Standard imports
import json
from os import scandir
from pathlib import Path
import pickle
import unittest

# Third-party imports
import numpy as np
import pandas as pd

# Local imports
from input.Extract import Extract, calculate_d_x_a, create_reach_dict, create_node_dict, extract_node_local, extract_reach_local

class TestExtract(unittest.TestCase):
    """Tests methods and functions from Extract module."""

    def test_append_node(self):
        """Tests append_node method."""

        reach = Path(__file__).parent / "test_data" / "reach_data"
        with open(reach, "rb") as pf:
            reach_dict = pickle.load(pf)
        node = Path(__file__).parent / "test_data" / "node_data"
        with open(node, "rb") as pf:
            node_dict = pickle.load(pf)

        indexes = np.array(node_dict["na"]["slope2"].index)
        r_ids = node_dict["na"]["slope2"]["reach_id"].to_numpy()
        df = pd.DataFrame(data=indexes, columns=["node_id"]).set_index("node_id")
        df.insert(loc=0, column="reach_id", value=r_ids)
        node_dict["na"]["slope2"] = df

        ext = Extract()
        ext.reach_data = reach_dict
        ext.node_data = node_dict
        ext.append_node("slope2", 25)

        expected_slope = np.array([0.00010045794, np.nan, 0.00010173043, 9.540541e-05, np.nan, 0.00011160423, 9.765124e-05, np.nan, 0.00010503138, 8.985157e-05, np.nan, 9.279268e-05, 0.00010460104, np.nan, 0.00010018548, 0.00013338136, np.nan, 0.00010086814, 0.00011058383, np.nan, 9.262967e-05, 1.900279e-05, np.nan, 6.059819e-05, 8.804341e-05])
        actual_slope = ext.node_data["na"]["slope2"].loc[ext.node_data["na"]["slope2"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        self.assertTrue(np.allclose(expected_slope, actual_slope, equal_nan=True))

    def test_calculate_d_x_a(self):
        """Tests calculate_d_x_a function."""

        reach = Path(__file__).parent / "test_data" / "reach_data"
        with open(reach, "rb") as pf:
            reach_dict = pickle.load(pf)

        width = reach_dict["na"]["width"].loc["77449100161"].iloc[:3]
        wse = reach_dict["na"]["wse"].loc["77449100161"].iloc[:3]
        actual_dxa = calculate_d_x_a(wse, width)

        expected_dxa = pd.Series([8.579490729, -50.358241744, 0])
        expected_dxa.name = "77449100161"
        pd.testing.assert_series_equal(expected_dxa, actual_dxa, atol=1e-2)

    def test_extract_data_local(self):
        """Tests exctract_data_local method."""

        input = Path(__file__).parent / "test_sf"
        with scandir(input) as entries:
            dirs = sorted([ entry.path for entry in entries ])
        ext = Extract()
        ext.extract_data_local(dirs)
        
        # reach-level data
        reach = ext.reach_data["na"]
        self.assertEqual(9, reach["nt"])
        
        expected_width = np.array([79.981045, 102.228203, 131.835053, 72.060132, 73.473868, 73.547419, 74.904443, 73.93421, 87.579335])
        np.testing.assert_array_almost_equal(expected_width, reach["width"].loc["77449100061"].to_numpy())

        expected_wse = np.array([7.99663, 8.09615, 14.86537, 8.91665, 8.16795, 8.29723, 8.7095, 8.53303, 8.92249])
        np.testing.assert_array_almost_equal(expected_wse, reach["wse"].loc["77449100061"].to_numpy())

        expected_slope = np.array([0.00010045794, 9.540541e-05, 9.765124e-05, 8.985157e-05, 0.00010460104, 0.00013338136, 0.00011058383, 1.900279e-05, 8.804341e-05])
        np.testing.assert_array_almost_equal(expected_slope, reach["slope2"].loc["77449100061"].to_numpy())

        # node-level data
        node = ext.node_data["na"]
        self.assertEqual(9, node["nt"])

        expected_width = np.array([43.208299, 62.901061, 85.893091, 44.241324, 49.556449, 30.668317, 60.893438, 50.442531, 44.137341])
        actual_width = node["width"].loc[node["width"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        np.testing.assert_array_almost_equal(expected_width, actual_width)

        expected_wse = np.array([7.64898, 7.65856, 14.55173, 8.47558, 7.55344, 8.10743, 8.22878, 7.98136, 8.5523])
        actual_wse = node["wse"].loc[node["wse"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        np.testing.assert_array_almost_equal(expected_wse, actual_wse)

        actual_slope = node["slope2"].loc[node["slope2"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        np.testing.assert_array_almost_equal(expected_slope, actual_slope)

    def test_extract_node_local(self):
        """Tests extract_node_local function."""

        node_path = Path(__file__).parent / "test_sf" / "109"/ "riverobs_nominal_20201105" / "river_data" / "nodes.shp"
        with open(Path(__file__).parent.parent / "src" / "data" / "sac.json") as f:
            sac_node = json.load(f)["sac_nodes"]
        time = 0
        node_dict = create_node_dict(sac_node)
        extract_node_local(node_path, node_dict, time)
        
        expected_reach = np.full((49), fill_value="77449100061")
        actual_reach = node_dict["wse"].loc[node_dict["wse"]["reach_id"] == "77449100061"]["reach_id"].to_numpy()
        np.testing.assert_array_equal(expected_reach, actual_reach)

        expected_node = np.array(list(sac_node.keys()))
        actual_node = np.array(node_dict["wse"].index)
        np.testing.assert_array_equal(expected_node, actual_node)

        expected_width = np.array([43.208299, 69.361156, 76.104040, 92.461620, 76.389673])
        actual_width = node_dict["width"].loc[node_dict["width"]["reach_id"] == "77449100061"].iloc[:5][0].to_numpy()
        np.testing.assert_array_almost_equal(expected_width, actual_width)
        
        expected_wse = np.array([7.64898, 7.42904, 7.48903, 7.49127, 7.71399])
        actual_wse = node_dict["wse"].loc[node_dict["wse"]["reach_id"] == "77449100061"].iloc[:5][0].to_numpy()
        np.testing.assert_array_almost_equal(expected_wse, actual_wse)

        node_path = Path(__file__).parent / "test_sf" / "130"/ "riverobs_nominal_20201105" / "river_data" / "nodes.shp"
        time = 1
        extract_node_local(node_path, node_dict, time)

        expected_width = np.array([43.208299, 62.901061, 69.361156, 58.018872, 76.104040, 63.814723, 92.461620, 76.794963, 76.389673, 90.156724])
        expected_width = np.reshape(expected_width, (5,2))
        actual_width = node_dict["width"].loc[node_dict["width"]["reach_id"] == "77449100061"].loc[:,0:].iloc[:5].to_numpy()
        np.testing.assert_array_almost_equal(expected_width, actual_width)

        expected_wse = np.array([7.64898, 7.65856, 7.42904, 7.56269, 7.48903, 7.48027, 7.49127, 7.78397, 7.71399, 7.66650])
        expected_wse = np.reshape(expected_wse, (5,2))
        actual_wse = node_dict["wse"].loc[node_dict["wse"]["reach_id"] == "77449100061"].loc[:,0:].iloc[:5].to_numpy()
        np.testing.assert_array_almost_equal(expected_wse, actual_wse)

    def test_extract_reach_local(self):
        """Tests extract_reach_local funtion."""

        reach_path = Path(__file__).parent / "test_sf" / "109"/ "riverobs_nominal_20201105" / "river_data" / "reaches.shp"
        with open(Path(__file__).parent.parent / "src" / "data" / "sac.json") as f:
            sac_reach = json.load(f)["sac_reaches"]
        time = 0
        reach_dict = create_reach_dict(sac_reach)
        extract_reach_local(reach_path, reach_dict, time)

        self.assertAlmostEqual(79.981045, reach_dict["width"].loc["77449100061"].iloc[0])
        self.assertAlmostEqual(7.99663, reach_dict["wse"].loc["77449100061"].iloc[0])
        self.assertAlmostEqual( 0.00010045794, reach_dict["slope2"].loc["77449100061"].iloc[0], places=7)

        reach_path = Path(__file__).parent / "test_sf" / "130"/ "riverobs_nominal_20201105" / "river_data" / "reaches.shp"
        time = 1
        extract_reach_local(reach_path, reach_dict, time)

        expected_width = pd.Series([79.981045, 102.228203])
        expected_width.name = "77449100061"
        pd.testing.assert_series_equal(expected_width, reach_dict["width"].loc["77449100061"])

        expected_wse = pd.Series([7.99663, 8.09615])
        expected_wse.name = "77449100061"
        pd.testing.assert_series_equal(expected_wse, reach_dict["wse"].loc["77449100061"])

        expected_slope = pd.Series([0.00010045794, 9.540541e-05])
        expected_slope.name = "77449100061"
        pd.testing.assert_series_equal(expected_slope, reach_dict["slope2"].loc["77449100061"])