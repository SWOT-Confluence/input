# Standard imports
from pathlib import Path
from shutil import rmtree
import unittest

# Third-party imports
from netCDF4 import Dataset, chartostring
import numpy as np
from numpy.testing import assert_array_almost_equal, assert_array_equal

# Local imports
from input.Extract import Extract
from input.Write import Write

class TestWrite(unittest.TestCase):
    """Tests methods from Write class."""

    def test_write_data(self):
        """Tests write_data method."""
        
        # Obtain required data
        node_list = ["74267100010071", "74267100010081", "74267100010091", "74267100010101", "74267100010111"]
        Extract.LOCAL_INPUT = swot_dir = Path(__file__).parent / "test_data"
        ext = Extract(None, "74267100011", node_list)
        ext.extract_data_local()

        # Set up I/O, create Write object and execute function
        swot_dir = Path(__file__).parent / "swot"
        if not swot_dir.exists(): swot_dir.mkdir(parents=True, exist_ok=True)
        write = Write(ext.node_data, ext.reach_data, ext.obs_times, swot_dir.parent)
        write.write_data("74267100011", node_list)
        
        # Assert file results
        dataset = Dataset(swot_dir / "74267100011_SWOT.nc", 'r')
        
        # Global data
        self.assertEqual(5, dataset.dimensions["nx"].size)
        self.assertEqual(5, dataset.dimensions["nt"].size)
        self.assertEqual("NA", dataset.continent)
        assert_array_equal(["1/441", "1/456", "2/441", "2/456", "3/441"], chartostring(dataset["observations"][:]))
        
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