# Standard imports
import glob
from pathlib import Path
import unittest
from unittest.mock import patch

# Third-party imports
import geopandas as gpd
import numpy as np
from numpy.testing import assert_array_almost_equal, assert_array_equal
from s3fs import S3FileSystem

# Local imports
from input.Extract import Extract, calculate_d_x_a, create_node_dict, extract_passes, extract_passes_local

class TestExtract(unittest.TestCase):
    """Tests methods and functions from Extract module."""
    
    PARENT = Path(__file__).parent / "test_data"
    REACH_ID = "74267100011"
    NODE_LIST = ["74267100010071", "74267100010081", "74267100010091", "74267100010101", "74267100010111"]
    
    def get_file_list(self):
        """Helper function that returns list of test files."""
        
        return [Path(c_file).name for c_file in glob.glob(str(self.PARENT / f"*reach*.shp"))]
    
    def test_calculate_d_x_a(self):
        """Tests calculate_d_x_a function."""

        wse = np.array([103, 102, 101, 102, 104], dtype=np.float64)
        width = np.array([620, 713, 628, 631, 615], dtype=np.float64)
        d_x_area = calculate_d_x_a(wse, width)
        expected = np.array([620, 0, -628, 0, 1230], dtype=np.float64)
        assert_array_almost_equal(expected, d_x_area)
    
    @patch.object(S3FileSystem, "glob") 
    def test_extract_passes(self, mock_fs):
        """Tests extract_passes function."""
        
        mock_fs.glob.return_value = self.get_file_list()
        c_dict = extract_passes(7, mock_fs)
        expected = {
            1: [441, 456],
            2: [441, 456],
            3: [441]
        }
        self.assertEqual(expected, c_dict)
        
    def test_extract_passes_local(self):
        """Tests extract_passes_local function."""
        
        c_dict = extract_passes_local(7, Path(__file__).parent / "test_data")
        expected = {
            1: [441, 456],
            2: [441, 456],
            3: [441]
        }
        self.assertEqual(expected, c_dict)

    @patch.object(S3FileSystem, "glob")
    def test_append_node(self, mock_fs):
        """Tests append_node method."""
        
        # Create Extract object
        mock_fs.glob.return_value = self.get_file_list()
        node_ids = ["74267100010011", "74267100010021", "74267100010031", "74267100010041", "74267100010051"]
        ext = Extract(mock_fs, "74267100011", node_ids)
        
        # Set and append reach data to node level data
        ext.reach_data["slope2"] = [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05]
        ext.append_node("slope2", len(node_ids))
        
        # Assert results
        self.assertEqual((5,10), ext.node_data["slope2"].shape)
        expected = [[4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05],
                    [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05],
                    [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05],
                    [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05],
                    [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05, 4.1e-05, 3.5e-05, 3.5e-05, 4.4e-05, 4.4e-05]]
        assert_array_almost_equal(expected, ext.node_data["slope2"])
    
    @patch.object(S3FileSystem, "glob")    
    def test_extract_data(self, mock_fs):
        """Tests extract_data method."""
        
        # Create object and extract data
        reach_files = self.get_file_list()
        reach_files.sort()
        reach_dfs = [gpd.read_file(str(self.PARENT / reach_file)) for reach_file in reach_files]
        node_files = [Path(c_file).name for c_file in glob.glob(str(self.PARENT / f"*node*.shp"))]
        node_files.sort()
        node_dfs = [gpd.read_file(str(self.PARENT / node_file)) for node_file in node_files]
        file_list = [reach_files] + reach_files + node_files
        df_list = reach_dfs + node_dfs
        
        Extract.LOCAL_INPUT = Path(__file__).parent / "test_data"
        mock_fs.glob.side_effect = file_list
        ext = Extract(mock_fs, self.REACH_ID, self.NODE_LIST)
        with patch.object(gpd, "read_file") as mock_gpd:
            mock_gpd.side_effect = df_list
            ext.extract_data()
        
        # Assert reach-level results
        expected = np.array([620.376586, 620.376586, 713.994386, 713.994386, 628.685508])
        assert_array_almost_equal(expected, ext.reach_data["width"])
        expected = np.array([103.159082, 103.159082, 102.740695, 102.740695, 103.151996])
        assert_array_almost_equal(expected, ext.reach_data["wse"])
        expected = np.array([4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05])
        assert_array_almost_equal(expected, ext.reach_data["slope2"])
        expected = np.array([0, 0, 0, 0, 0])
        assert_array_almost_equal(expected, ext.reach_data["reach_q"])
        expected = np.array([4.39598849, 4.39598849, -293.66660496, -293.66660496, 0])
        assert_array_almost_equal(expected, ext.reach_data["d_x_area"])
        
        # Assert node-level results
        expected = np.array([[4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05]])
        assert_array_almost_equal(expected, ext.node_data["slope2"])
        expected = np.array([[0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017]])
        assert_array_almost_equal(expected, ext.node_data["slope2_u"])
        expected = np.array([[4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0]])
        assert_array_almost_equal(expected, ext.node_data["d_x_area"])
        expected = np.array([[10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1]])
        assert_array_almost_equal(expected, ext.node_data["d_x_area_u"])
        expected = np.array([[619.002555, 619.002555, 605.115039, 605.115039, 582.430011],
                             [610.590518, 610.590518, 610.341479, 610.341479, 589.098464],
                             [630.325838, 630.325838, 585.900209, 585.900209, 610.681111],
                             [611.293345, 611.293345, 636.617022, 636.617022, 604.398205],
                             [615.947963, 615.947963, 617.022999, 617.022999, 600.295217]])
        assert_array_almost_equal(expected, ext.node_data["width"])
        expected = np.array([[103.802578, 103.802578, 103.317976, 103.317976, 102.976548],
                             [103.795217, 103.795217, 103.30622, 103.30622, 102.967348],
                             [103.786274, 103.786274, 103.298996, 103.298996, 102.956941],
                             [103.795423, 103.795423, 103.304711, 103.304711, 102.966301],
                             [103.816421, 103.816421, 103.322611, 103.322611, 102.984772]])
        assert_array_almost_equal(expected, ext.node_data["wse"])
        expected = np.full((5,5), fill_value=0, dtype=int)
        assert_array_almost_equal(expected, ext.node_data["node_q"])
        
        # Time data
        expected = ["1/441", "1/456", "2/441", "2/456", "3/441"]
        assert_array_equal(expected, ext.obs_times)
    
    @patch.object(S3FileSystem, "glob")    
    def test_extract_data_local(self, mock_fs):
        """Tests extract_data_local method."""
        
        # Create object and extract data
        reach_files = self.get_file_list()
        reach_files.sort()
        reach_dfs = [gpd.read_file(str(self.PARENT / reach_file)) for reach_file in reach_files]
        node_files = [Path(c_file).name for c_file in glob.glob(str(self.PARENT / f"*node*.shp"))]
        node_files.sort()
        node_dfs = [gpd.read_file(str(self.PARENT / node_file)) for node_file in node_files]
        df_list = reach_dfs + node_dfs
        
        Extract.LOCAL_INPUT = Path(__file__).parent / "test_data"
        mock_fs.glob.return_value = self.get_file_list()
        ext = Extract(mock_fs, self.REACH_ID, self.NODE_LIST)
        with patch.object(gpd, "read_file") as mock_gpd:
            mock_gpd.side_effect = df_list
            ext.extract_data_local()
        
        # Assert reach-level results
        expected = np.array([620.376586, 620.376586, 713.994386, 713.994386, 628.685508])
        assert_array_almost_equal(expected, ext.reach_data["width"])
        expected = np.array([103.159082, 103.159082, 102.740695, 102.740695, 103.151996])
        assert_array_almost_equal(expected, ext.reach_data["wse"])
        expected = np.array([4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05])
        assert_array_almost_equal(expected, ext.reach_data["slope2"])
        expected = np.array([0, 0, 0, 0, 0])
        assert_array_almost_equal(expected, ext.reach_data["reach_q"])
        expected = np.array([4.39598849, 4.39598849, -293.66660496, -293.66660496, 0])
        assert_array_almost_equal(expected, ext.reach_data["d_x_area"])
        
        # Assert node-level results
        expected = np.array([[4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05]])
        assert_array_almost_equal(expected, ext.node_data["slope2"])
        expected = np.array([[0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017]])
        assert_array_almost_equal(expected, ext.node_data["slope2_u"])
        expected = np.array([[4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0]])
        assert_array_almost_equal(expected, ext.node_data["d_x_area"])
        expected = np.array([[10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1]])
        assert_array_almost_equal(expected, ext.node_data["d_x_area_u"])
        expected = np.array([[619.002555, 619.002555, 605.115039, 605.115039, 582.430011],
                             [610.590518, 610.590518, 610.341479, 610.341479, 589.098464],
                             [630.325838, 630.325838, 585.900209, 585.900209, 610.681111],
                             [611.293345, 611.293345, 636.617022, 636.617022, 604.398205],
                             [615.947963, 615.947963, 617.022999, 617.022999, 600.295217]])
        assert_array_almost_equal(expected, ext.node_data["width"])
        expected = np.array([[103.802578, 103.802578, 103.317976, 103.317976, 102.976548],
                             [103.795217, 103.795217, 103.30622, 103.30622, 102.967348],
                             [103.786274, 103.786274, 103.298996, 103.298996, 102.956941],
                             [103.795423, 103.795423, 103.304711, 103.304711, 102.966301],
                             [103.816421, 103.816421, 103.322611, 103.322611, 102.984772]])
        assert_array_almost_equal(expected, ext.node_data["wse"])
        expected = np.full((5,5), fill_value=0, dtype=int)
        assert_array_almost_equal(expected, ext.node_data["node_q"])
        
        # Time data
        expected = ["1/441", "1/456", "2/441", "2/456", "3/441"]
        assert_array_equal(expected, ext.obs_times)
    
    @patch.object(S3FileSystem, "glob")    
    def test_extract_node(self, mock_fs):
        """Tests extract_node method."""
        
        mock_fs.glob.return_value = self.get_file_list()
        ext = Extract(mock_fs, self.REACH_ID, self.NODE_LIST)
        ext.node_data = create_node_dict(5,5)
        node_file = Path(__file__).parent / "test_data" /  "SWOT_L2_HR_RiverSP_node_1_441_NA_20100214T170527_20100214T170537_PGA2_03.shp"
        df = gpd.read_file(node_file)
        with patch.object(gpd, "read_file") as mock_gpd:
            mock_gpd.return_value = df
            ext.extract_node(node_file, 0)
        
        expected = np.array([[619.002555, np.nan, np.nan, np.nan, np.nan],
                             [610.590518, np.nan, np.nan, np.nan, np.nan],
                             [630.325838, np.nan, np.nan, np.nan, np.nan],
                             [611.293345, np.nan, np.nan, np.nan, np.nan],
                             [615.947963, np.nan, np.nan, np.nan, np.nan]])
        assert_array_almost_equal(expected, ext.node_data["width"])
        
        expected = np.array([[103.802578, np.nan, np.nan, np.nan, np.nan],
                             [103.795217, np.nan, np.nan, np.nan, np.nan],
                             [103.786274, np.nan, np.nan, np.nan, np.nan],
                             [103.795423, np.nan, np.nan, np.nan, np.nan],
                             [103.816421, np.nan, np.nan, np.nan, np.nan]])
        assert_array_almost_equal(expected, ext.node_data["wse"])
        
        expected = np.array([[0, -999, -999, -999, -999],
                             [0, -999, -999, -999, -999],
                             [0, -999, -999, -999, -999],
                             [0, -999, -999, -999, -999],
                             [0, -999, -999, -999, -999]])
        assert_array_almost_equal(expected, ext.node_data["node_q"])
    
    @patch.object(S3FileSystem, "glob")    
    def test_extract_reach(self, mock_fs):
        """Tests extract_reach method."""
        
        mock_fs.glob.return_value = self.get_file_list()
        ext = Extract(mock_fs, self.REACH_ID, self.NODE_LIST)
        
        reach_file = Path(__file__).parent / "test_data" / "SWOT_L2_HR_RiverSP_reach_1_441_NA_20100214T170530_20100215T060108_PGA2_03.shp"
        df = gpd.read_file(reach_file)        
        with patch.object(gpd, "read_file") as mock_gpd:
            mock_gpd.return_value = df
            ext.extract_reach(reach_file)
        
        self.assertAlmostEqual([620.376586], ext.reach_data["width"])
        self.assertAlmostEqual([103.159082], ext.reach_data["wse"])
        self.assertAlmostEqual([0.000045], ext.reach_data["slope2"])
        self.assertAlmostEqual([0], ext.reach_data["d_x_area"])
        self.assertAlmostEqual([0], ext.reach_data["reach_q"])