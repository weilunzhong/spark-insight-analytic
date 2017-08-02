import unittest
from datetime import datetime
from diagnostic.data_interface.input_data import SparkParquetIO

class TestUCIModel(unittest.TestCase):

    def test_load_interactions(self):
        dt = datetime(2017, 1, 1)
        spark = SparkParquetIO()
        interactions = spark.get_daily_interactions(dt)
        print interactions.count()


if __name__ == "__main__":
    unittest.main()
