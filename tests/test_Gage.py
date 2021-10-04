# Standard imports
from os import mkdir
from pathlib import Path
import pickle
from shutil import copyfile, rmtree
import unittest

# Third-party imports
from netCDF4 import Dataset, chartostring
import numpy as np
from numpy.testing import assert_almost_equal, assert_array_equal

# Local imports
from input.gage_pull.GageAppend import GageAppend

class test_Gage(unittest.TestCase):
    """Test GageAppend operations."""

    SOS_DIR = Path(__file__).parent / "test_sos"
    APPEND_DIR = Path(__file__).parent / "append"
    USGS_FILE = Path(__file__).parent / "test_data" / "usgs_data"

    def test_map_data(self):
        """Test map_data method."""

        # Load data
        with open(self.USGS_FILE, "rb") as f:
            usgs_dict = pickle.load(f)

        # Run method
        gage_append = GageAppend(self.SOS_DIR, usgs_dict)
        gage_append.read_sos()
        gage_append.map_data()

        na = gage_append.map_dict["na"]
        i = np.where(na["usgs_reach_id"] == 81130400041)
        self.assertAlmostEqual(17.6696832, na["min_q"][i])
        self.assertAlmostEqual(809.8604799999999, na["max_q"][i])
        self.assertAlmostEqual(155.55638379236225, na["mean_q"][i])
        self.assertAlmostEqual(472.89056, na["tyr"][i])
        fdq = np.array([487.04896, 393.60352, 356.79168, 328.47488, 300.15808, 267.876928, 227.95024, 180.944352, 139.884992, 108.453344, 84.667232, 69.092992, 58.898944, 51.253408, 45.023712, 39.077184, 33.98016, 30.582144, 27.2690784, 23.7294784])
        assert_almost_equal(fdq, na["fdq"][i,:].flatten())
        monthly_q = np.array([49.04572523, 40.21407428, 30.72144439, 30.46769693, 77.20958898, 236.66048768, 358.41769521, 349.62698153, 293.00923598, 196.50184551, 107.47680769, 66.08166778])
        assert_almost_equal(monthly_q, na["monthly_q"][i,:].flatten())
        self.assertEqual("15266110", na["usgs_id"][i])
        grdc_q = [238.427456, 237.011616, 232.480928, 232.19776, 225.684896, 218.03936, 210.393824, 202.748288, 194.253248, np.nan]
        assert_almost_equal(grdc_q, na["usgs_q"][i,-10:].flatten())
        grdc_qt = [738046, 738047, 738048, 738049, 738050, 738051, 738052, 738053, 738054, np.nan]
        assert_almost_equal(grdc_qt, na["usgs_qt"][i,-10:].flatten())
        assert_array_equal(np.array(range(1,15241)), na["days"])

    def test_append_data(self):
        """Test append_data method."""

        # Copy and load data needed for test
        file_name = "na_sword_v11_SOS.nc"
        if not self.APPEND_DIR.exists(): self.APPEND_DIR.mkdir(parents=True, exist_ok=True)
        copyfile(self.SOS_DIR / file_name, self.APPEND_DIR / file_name)

        with open(self.USGS_FILE, "rb") as f:
            usgs_dict = pickle.load(f)

        # Create results
        gage_append = GageAppend(self.APPEND_DIR, usgs_dict)
        gage_append.read_sos()
        gage_append.map_data()
        gage_append.append_data()

        # Assert results
        sf = self.APPEND_DIR / "na_sword_v11_SOS.nc"
        dataset = Dataset(sf)

         # attributes
        self.assertEqual("0000", dataset.version)
        self.assertEqual("constrained", dataset.run_type)

        # dimensions
        self.assertEqual(1642238, dataset.dimensions["num_nodes"].size)
        self.assertEqual(37565, dataset.dimensions["num_reaches"].size)
        self.assertEqual(1, dataset.dimensions["time_steps"].size)
        self.assertEqual(12, dataset["model"].dimensions["num_months"].size)
        self.assertEqual(20, dataset["model"].dimensions["probability"].size)
        self.assertEqual(15240, dataset["model"]["usgs"].dimensions["num_days"].size)
        self.assertEqual(10, dataset["model"]["usgs"].dimensions["num_usgs_reaches"].size)

        # data
        usgs = dataset["model"]["usgs"]
        i = np.where(usgs["usgs_reach_id"][:] == 81130400041)
        self.assertAlmostEqual(17.6696832, usgs["min_q"][i])
        self.assertAlmostEqual(809.8604799999999, usgs["max_q"][i])
        self.assertAlmostEqual(155.55638379236225, usgs["mean_q"][i])
        self.assertAlmostEqual(472.89056, usgs["two_year_return_q"][i])
        fdq = np.array([487.04896, 393.60352, 356.79168, 328.47488, 300.15808, 267.876928, 227.95024, 180.944352, 139.884992, 108.453344, 84.667232, 69.092992, 58.898944, 51.253408, 45.023712, 39.077184, 33.98016, 30.582144, 27.2690784, 23.7294784])
        assert_almost_equal(fdq, usgs["flow_duration_q"][i].flatten())
        monthly_q = np.array([49.04572523, 40.21407428, 30.72144439, 30.46769693, 77.20958898, 236.66048768, 358.41769521, 349.62698153, 293.00923598, 196.50184551, 107.47680769, 66.08166778])
        assert_almost_equal(monthly_q, usgs["monthly_q"][i].flatten())
        self.assertEqual(["15266110"], chartostring(usgs["usgs_id"][i]))
        grdc_q = [238.427456, 237.011616, 232.480928, 232.19776, 225.684896, 218.03936, 210.393824, 202.748288, 194.253248, np.nan]
        assert_almost_equal(grdc_q, usgs["usgs_q"][:][i,-10:].flatten())
        grdc_qt = [738046, 738047, 738048, 738049, 738050, 738051, 738052, 738053, 738054, np.nan]
        assert_almost_equal(grdc_qt, usgs["usgs_qt"][:][i,-10:].flatten())
        assert_array_equal(np.array(range(1,15241)), usgs["num_days"])

        dataset.close()
        rmtree(self.APPEND_DIR)