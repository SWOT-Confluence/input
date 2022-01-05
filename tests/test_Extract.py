# Standard imports
from pathlib import Path
import unittest

# Third-party imports
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from input.Extract import Extract, calculate_d_x_a, create_node_dict, extract_passes_local

class TestExtract(unittest.TestCase):
    """Tests methods and functions from Extract module."""
    
    REACH_ID = "74267100011"
    NODE_LIST = ["74267100010071", "74267100010081", "74267100010091", "74267100010101", "74267100010111"]
    
    def test_calculate_d_x_a(self):
        """Tests calculate_d_x_a function."""

        wse = np.array([103, 102, 101, 102, 104], dtype=np.float64)
        width = np.array([620, 713, 628, 631, 615], dtype=np.float64)
        d_x_area = calculate_d_x_a(wse, width)
        expected = np.array([620, 0, -628, 0, 1230], dtype=np.float64)
        assert_array_almost_equal(expected, d_x_area)
        
    def test_extract_passes(self):
        """Tests extract_passes function."""
        
        c_dict = extract_passes_local(7, Path(__file__).parent / "test_data")
        expected = {
            1: [441, 456],
            2: [441, 456],
            3: [441]
        }
        self.assertEqual(expected, c_dict)

    def test_append_node(self):
        """Tests append_node method."""

        # Create Extract object
        node_ids = ["74267100010011", "74267100010021", "74267100010031", "74267100010041", "74267100010051"]
        ext = Extract(None, "74267100011", node_ids)
        
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
        
    def test_extract_data(self):
        """Tests extract_data method."""
        
        # Create object and extract data
        Extract.LOCAL_INPUT = Path(__file__).parent / "test_data"
        ext = Extract(None, self.REACH_ID, self.NODE_LIST)
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
        
    def test_extract_node(self):
        """Tests extract_node method."""
        
        ext = Extract(None, self.REACH_ID, self.NODE_LIST)
        ext.node_data = create_node_dict(5,5)
        ext.extract_node(Path(__file__).parent / "test_data" /  "SWOT_L2_HR_RiverSP_node_1_441_NA_20100214T170527_20100214T170537_PGA2_03.shp", 0)
        
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
        
    def test_extract_reach(self):
        """Tests extract_reach method."""
        
        ext = Extract(None, self.REACH_ID, self.NODE_LIST)
        ext.extract_reach(Path(__file__).parent / "test_data" / "SWOT_L2_HR_RiverSP_reach_1_441_NA_20100214T170530_20100215T060108_PGA2_03.shp")
        
        self.assertAlmostEqual([620.376586], ext.reach_data["width"])
        self.assertAlmostEqual([103.159082], ext.reach_data["wse"])
        self.assertAlmostEqual([0.000045], ext.reach_data["slope2"])
        self.assertAlmostEqual([0], ext.reach_data["d_x_area"])
        self.assertAlmostEqual([0], ext.reach_data["reach_q"])