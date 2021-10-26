# Standard imports
from datetime import date
import json
from os import listdir, scandir
from pathlib import Path
import pickle
import unittest
from unittest.mock import patch
import geopandas

# Third-party imports
import geopandas as gpd
import numpy as np
import pandas as pd
from s3fs import S3FileSystem

# Local imports
from input.Extract import Extract, calculate_d_x_a, create_reach_dict, create_node_dict, extract_node, extract_reach

class TestExtract(unittest.TestCase):
    """Tests methods and functions from Extract module."""

    def test_append_node(self):
        """Tests append_node method."""

        reach = Path(__file__).parent / "test_data" / "extract_reach_data"
        with open(reach, "rb") as pf:
            reach_dict = pickle.load(pf)
        node = Path(__file__).parent / "test_data" / "extract_node_data"
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

        reach = Path(__file__).parent / "test_data" / "extract_reach_data"
        with open(reach, "rb") as pf:
            reach_dict = pickle.load(pf)

        width = reach_dict["na"]["width"].loc["77449100161"].iloc[:3]
        wse = reach_dict["na"]["wse"].loc["77449100161"].iloc[:3]
        actual_dxa = calculate_d_x_a(wse, width)

        expected_dxa = pd.Series([8.579490729, -50.358241744, 0])
        expected_dxa.name = "77449100161"
        pd.testing.assert_series_equal(expected_dxa, actual_dxa, atol=1e-2)

    @patch.object(S3FileSystem, "ls")
    @patch.object(Extract, "TIME_DICT")
    def test_extract_data(self, mock_time, mock_fs):
        """Tests extract_data method."""
        
        # Mock API calls and time dictionary
        mock_fs.ls.side_effect = [[Path(__file__).parent / "test_sf" / "pass249"], ["test_sf/pass249/109", "test_sf/pass249/130", "test_sf/pass249/220", "test_sf/pass249/313", "test_sf/pass249/403", "test_sf/pass249/424", "test_sf/pass249/515", "test_sf/pass249/605", "test_sf/pass249/626"]]
        reach_path = Path(__file__).parent / "test_sf" / "pass249" / "109"/ "riverobs_nominal_20201105" / "river_data" / "reaches.shp"
        mock_fs.glob.return_value = [str(reach_path)]
        key = str(reach_path).split('/')[2]
        mock_time = {key: date(2009,6,26)}
        
        df_list = []
        with scandir(Path(__file__).parent / "test_sf" / "pass249") as entries:
            dirs = [ entry.name for entry in entries]
        
        dirs = sorted(dirs)
        for d in dirs:
            reach = Path(__file__).parent / "test_sf" / "pass249" / d / "riverobs_nominal_20201105" / "river_data" / f"{d}_reaches.shp"
            df_list.append(gpd.read_file(reach))
            node = Path(__file__).parent / "test_sf" / "pass249" / d / "riverobs_nominal_20201105" / "river_data" / f"{d}_nodes.shp"
            df_list.append(gpd.read_file(node))
        
        # Run method
        ext = Extract()
        with patch.object(geopandas, "read_file") as mock_gpd:
            mock_gpd.side_effect = df_list
            ext.extract_data(mock_fs)

        # reach-level data
        reach = ext.reach_data["na"]
        self.assertEqual(9, len(reach["time"]))
        
        expected_width = np.array([79.981045, 102.228203, 131.835053, 72.060132, 73.473868, 73.547419, 74.904443, 73.93421, 87.579335])
        np.testing.assert_array_almost_equal(expected_width, reach["width"].loc["77449100061"].to_numpy())

        expected_width_u = np.array([0.939375, 1.545424, 0.967832, 0.683558, 0.666685, 0.698807, 0.735233, 0.745103, 0.950822])
        np.testing.assert_array_almost_equal(expected_width_u, reach["width_u"].loc["77449100061"].to_numpy())

        expected_wse = np.array([7.99663, 8.09615, 14.86537, 8.91665, 8.16795, 8.29723, 8.7095, 8.53303, 8.92249])
        np.testing.assert_array_almost_equal(expected_wse, reach["wse"].loc["77449100061"].to_numpy())

        expected_slope = np.array([0.00010045794, 9.540541e-05, 9.765124e-05, 8.985157e-05, 0.00010460104, 0.00013338136, 0.00011058383, 1.900279e-05, 8.804341e-05])
        np.testing.assert_array_almost_equal(expected_slope, reach["slope2"].loc["77449100061"].to_numpy())

        expected_slope_u = np.array([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
        np.testing.assert_array_almost_equal(expected_slope_u, reach["slope2_u"].loc["77449100061"].to_numpy())

        expected_dxa_u = np.array([141508411.405, 141511250.586, 141609416.134, 141520995.491, 141510481.11, 141512313.117, 141518213.954, 141515667.59, 141521953.203])
        np.testing.assert_array_almost_equal(expected_dxa_u, reach["d_x_area_u"].loc["77449100061"].to_numpy())

        # node-level data
        node = ext.node_data["na"]
        self.assertEqual(9, len(reach["time"]))

        expected_width = np.array([43.208299, 62.901061, 85.893091, 44.241324, 49.556449, 30.668317, 60.893438, 50.442531, 44.137341])
        actual_width = node["width"].loc[node["width"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        np.testing.assert_array_almost_equal(expected_width, actual_width)

        expected_width_u = np.array([4.635789, 5.6747802, 4.6544526, 3.5315369, 3.5679297, 2.4884082, 5.7413614, 3.8802125, 4.4176386])
        actual_width_u = node["width_u"].loc[node["width_u"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        np.testing.assert_array_almost_equal(expected_width_u, actual_width_u)

        expected_wse = np.array([7.64898, 7.65856, 14.55173, 8.47558, 7.55344, 8.10743, 8.22878, 7.98136, 8.5523])
        actual_wse = node["wse"].loc[node["wse"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        np.testing.assert_array_almost_equal(expected_wse, actual_wse)

        actual_slope = node["slope2"].loc[node["slope2"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        np.testing.assert_array_almost_equal(expected_slope, actual_slope)

        actual_slope_u = node["slope2_u"].loc[node["slope2_u"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        np.testing.assert_array_almost_equal(expected_slope_u, actual_slope_u)

        actual_dxa_u = node["d_x_area_u"].loc[node["d_x_area_u"]["reach_id"] == "77449100061"].iloc[0,1:].to_numpy().astype(float)
        np.testing.assert_array_almost_equal(expected_dxa_u, actual_dxa_u)

    def test_extract_node(self):
        """Tests extract_node function."""

        node_path = Path(__file__).parent / "test_sf" / "pass249" / "109"/ "riverobs_nominal_20201105" / "river_data" / "nodes.shp"
        df1 = gpd.read_file(node_path)
        
        with open(Path(__file__).parent.parent / "input" / "data" / "sac.json") as f:
            sac_node = json.load(f)["sac_nodes"]
        
        time = 0
        node_dict = create_node_dict(sac_node)
        with patch.object(geopandas, "read_file") as mock_gpd:
            mock_gpd.return_value = df1
            extract_node(node_path, node_dict, time)
        
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

        node_path = Path(__file__).parent / "test_sf" / "pass249" / "130"/ "riverobs_nominal_20201105" / "river_data" / "nodes.shp"
        df2 = gpd.read_file(node_path)
        time = 1
        with patch.object(geopandas, "read_file") as mock_gpd:
            mock_gpd.return_value = df2
            extract_node(node_path, node_dict, time)

        expected_width = np.array([43.208299, 62.901061, 69.361156, 58.018872, 76.104040, 63.814723, 92.461620, 76.794963, 76.389673, 90.156724])
        expected_width = np.reshape(expected_width, (5,2))
        actual_width = node_dict["width"].loc[node_dict["width"]["reach_id"] == "77449100061"].loc[:,0:].iloc[:5].to_numpy()
        np.testing.assert_array_almost_equal(expected_width, actual_width)

        expected_wse = np.array([7.64898, 7.65856, 7.42904, 7.56269, 7.48903, 7.48027, 7.49127, 7.78397, 7.71399, 7.66650])
        expected_wse = np.reshape(expected_wse, (5,2))
        actual_wse = node_dict["wse"].loc[node_dict["wse"]["reach_id"] == "77449100061"].loc[:,0:].iloc[:5].to_numpy()
        np.testing.assert_array_almost_equal(expected_wse, actual_wse)

    @patch.object(Extract, "TIME_DICT")
    def test_extract_reach(self, mock_time):
        """Tests extract_reach funtion."""

        reach_path = Path(__file__).parent / "test_sf" / "pass249" / "109"/ "riverobs_nominal_20201105" / "river_data" / "reaches.shp"
        df1 = gpd.read_file(reach_path)

        key = str(reach_path).split('/')[2]
        mock_time = {key: date(2009,6,26)}
        
        with open(Path(__file__).parent.parent / "input" / "data" / "sac.json") as f:
            sac_reach = json.load(f)["sac_reaches"]
        
        time = 0
        reach_dict = create_reach_dict(sac_reach)
        with patch.object(geopandas, "read_file") as mock_gpd:
            mock_gpd.return_value = df1
            extract_reach(str(reach_path), reach_dict, time)
        
        self.assertAlmostEqual(79.981045, reach_dict["width"].loc["77449100061"].iloc[0])
        self.assertAlmostEqual(7.99663, reach_dict["wse"].loc["77449100061"].iloc[0])
        self.assertAlmostEqual( 0.00010045794, reach_dict["slope2"].loc["77449100061"].iloc[0], places=7)

        reach_path = Path(__file__).parent / "test_sf" / "pass249" / "130"/ "riverobs_nominal_20201105" / "river_data" / "reaches.shp"
        df2 = gpd.read_file(reach_path)
        time = 1
        with patch.object(geopandas, "read_file") as mock_gpd:
            mock_gpd.return_value = df2
            extract_reach(str(reach_path), reach_dict, time)

        expected_width = pd.Series([79.981045, 102.228203])
        expected_width.name = "77449100061"
        pd.testing.assert_series_equal(expected_width, reach_dict["width"].loc["77449100061"])

        expected_wse = pd.Series([7.99663, 8.09615])
        expected_wse.name = "77449100061"
        pd.testing.assert_series_equal(expected_wse, reach_dict["wse"].loc["77449100061"])

        expected_slope = pd.Series([0.00010045794, 9.540541e-05])
        expected_slope.name = "77449100061"
        pd.testing.assert_series_equal(expected_slope, reach_dict["slope2"].loc["77449100061"])