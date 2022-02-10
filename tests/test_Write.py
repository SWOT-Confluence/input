# Standard imports
import glob
from pathlib import Path
from shutil import rmtree
import unittest
from unittest.mock import patch

# Third-party imports
import geopandas as gpd
from netCDF4 import Dataset, chartostring
import numpy as np
from numpy.testing import assert_array_almost_equal, assert_array_equal
from s3fs import S3FileSystem
from input.extract.ExtractLake import ExtractLake

# Local imports
from input.extract.ExtractRiver import ExtractRiver
from input.write.WriteLake import WriteLake
from input.write.WriteRiver import WriteRiver

class TestWrite(unittest.TestCase):
    """Tests methods from Write class."""
    
    REACH_ID = "74267100011"
    NODE_LIST = ["74267100010071", "74267100010081", "74267100010091", "74267100010101", "74267100010111"]
    LAKE_ID = "7720003433"
    
    @patch.object(S3FileSystem, "glob")
    def test_write_data_river(self, mock_fs):
        """Tests write_data method."""
        
        # Obtain required data
        parent = Path(__file__).parent / "test_data"
        reach_files = [Path(c_file).name for c_file in glob.glob(str(parent / f"*reach*.shp"))]
        reach_files.sort()
        reach_dfs = [gpd.read_file(str(parent / reach_file)) for reach_file in reach_files]
        
        node_files = [Path(c_file).name for c_file in glob.glob(str(parent / f"*node*.shp"))]
        node_files.sort()
        node_dfs = [gpd.read_file(str(parent / node_file)) for node_file in node_files]
        
        file_list = [reach_files] + reach_files + node_files
        df_list = reach_dfs + node_dfs  
        ExtractRiver.LOCAL_INPUT = Path(__file__).parent / "test_data"
        mock_fs.glob.side_effect = file_list
        ext = ExtractRiver(mock_fs, [self.REACH_ID, self.NODE_LIST], Path(__file__).parent / "test_json" / "cycle_passes-river-test.json")
        with patch.object(gpd, "read_file") as mock_gpd:
            mock_gpd.side_effect = df_list
            ext.extract()

        # Set up I/O, create Write object and execute function
        swot_dir = Path(__file__).parent / "swot"
        if not swot_dir.exists(): swot_dir.mkdir(parents=True, exist_ok=True)
        write = WriteRiver(self.REACH_ID, swot_dir.parent, self.NODE_LIST)
        write.write(ext.data, ext.obs_times)
        
        # Assert file results
        dataset = Dataset(swot_dir / "74267100011_SWOT.nc", 'r')
        
        # Global data
        self.assertEqual(5, dataset.dimensions["nx"].size)
        self.assertEqual(5, dataset.dimensions["nt"].size)
        self.assertEqual("NA", dataset.continent)
        assert_array_equal([1, 2, 3, 4, 5], dataset["observations"][:])
        
        # Reach data
        reach = dataset["reach"]
        self.assertEqual(74267100011, reach["reach_id"][:])
        expected = np.array([620.376586, 620.376586, 713.994386, 713.994386, 628.685508])
        assert_array_almost_equal(expected, reach["width"][:])
        expected = np.array([103.159082, 103.159082, 102.740695, 102.740695, 103.151996])
        assert_array_almost_equal(expected, reach["wse"][:])
        expected = np.array([4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05])
        assert_array_almost_equal(expected, reach["slope2"][:])
        expected = np.array([0, 0, 0, 0, 0])
        assert_array_almost_equal(expected, reach["reach_q"][:])
        expected = np.array([4.39598849, 4.39598849, -293.66660496, -293.66660496, 0])
        assert_array_almost_equal(expected, reach["d_x_area"][:])
        
        # Node data
        node = dataset["node"]
        self.assertEqual(74267100011, node["reach_id"][:])
        expected = np.array([74267100010071, 74267100010081, 74267100010091, 74267100010101, 74267100010111])
        assert_array_equal(expected, node["node_id"][:])
        expected = np.array([[4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05],
                             [4.5e-05, 4.5e-05, 3.9e-05, 3.9e-05, 4.1e-05]])
        assert_array_almost_equal(expected, node["slope2"][:])
        expected = np.array([[0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017],
                             [0.017, 0.017, 0.017, 0.017, 0.017]])
        assert_array_almost_equal(expected, node["slope2_u"][:])
        expected = np.array([[4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0],
                             [4.39598849, 4.39598849, -293.66660496, -293.66660496, 0]])
        assert_array_almost_equal(expected, node["d_x_area"][:])
        expected = np.array([[10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1],
                             [10.1, 10.1, 10.1, 10.1, 10.1]])
        assert_array_almost_equal(expected, node["d_x_area_u"][:])
        expected = np.array([[619.002555, 619.002555, 605.115039, 605.115039, 582.430011],
                             [610.590518, 610.590518, 610.341479, 610.341479, 589.098464],
                             [630.325838, 630.325838, 585.900209, 585.900209, 610.681111],
                             [611.293345, 611.293345, 636.617022, 636.617022, 604.398205],
                             [615.947963, 615.947963, 617.022999, 617.022999, 600.295217]])
        assert_array_almost_equal(expected, node["width"][:])
        expected = np.array([[103.802578, 103.802578, 103.317976, 103.317976, 102.976548],
                             [103.795217, 103.795217, 103.30622, 103.30622, 102.967348],
                             [103.786274, 103.786274, 103.298996, 103.298996, 102.956941],
                             [103.795423, 103.795423, 103.304711, 103.304711, 102.966301],
                             [103.816421, 103.816421, 103.322611, 103.322611, 102.984772]])
        assert_array_almost_equal(expected, node["wse"][:])
        expected = np.full((5,5), fill_value=0, dtype=int)
        assert_array_almost_equal(expected, node["node_q"][:])
        
        # Clean up
        dataset.close()
        rmtree(swot_dir)
        
    @patch.object(S3FileSystem, "glob")
    def test_write_data_lake(self, mock_fs):
        """Tests write_data method."""
        
        # Obtain required data
        parent = Path(__file__).parent / "test_data"
        lake_files = [Path(c_file).name for c_file in glob.glob(str(parent / f"*Prior*.shp"))]
        lake_files.sort()
        lake_dfs = [gpd.read_file(str(parent / lake_file)) for lake_file in lake_files]
        
        ExtractLake.LOCAL_INPUT = Path(__file__).parent / "test_data"
        mock_fs.glob.return_value = lake_files
        ext = ExtractLake(mock_fs, self.LAKE_ID, Path(__file__).parent / "test_json" / "cycle_passes-lake-test.json")
        with patch.object(gpd, "read_file") as mock_gpd:
            mock_gpd.side_effect = lake_dfs
            ext.extract()

        # Set up I/O, create Write object and execute function
        swot_dir = Path(__file__).parent / "swot"
        if not swot_dir.exists(): swot_dir.mkdir(parents=True, exist_ok=True)
        write = WriteLake(self.LAKE_ID, swot_dir.parent)
        write.write(ext.data, ext.obs_times)
        
        # Assert file results
        dataset = Dataset(swot_dir / "7720003433_SWOT.nc", 'r')
        
        # Global data
        self.assertEqual(5, dataset.dimensions["nt"].size)
        self.assertEqual("NA", dataset.continent)
        assert_array_equal([1, 2, 3, 4, 5], dataset["observations"][:])
        
        # Lake data
        expected = np.array(['2008-10-01', '2008-10-08', '2008-10-15', '2008-10-22', '2008-10-29'])
        assert_array_equal(expected, chartostring(dataset["time_str"][:]))
        
        expected = np.array([-2.39735586e-07, 6.5967069e-08, -1.11466267e-07, 5.9215381e-08, 1.39904948e-07])
        assert_array_almost_equal(expected, dataset["delta_s_q"][:])
        
        # Clean up
        dataset.close()
        rmtree(swot_dir)