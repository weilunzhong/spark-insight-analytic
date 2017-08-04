from pyspark.sql.types import *
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import desc, array_contains
from datetime import datetime, timedelta


# change this to os.genenv('PARQUET_PATH') for container usage
PATH = '/home/vionlabs/Documents/vionlabs_data/insight_data/telenor/parquet/{0}.parquet'
WAREHOUSE_DIR = '/home/vionlabs/Documents/vionlabs_data/insight_data/warehouse/'
UCI_START_DATE = datetime(2016, 12, 4)
UCI_END_DATE = datetime(2017, 6, 28)

# specify the schema to aviod scan all lines of file
interaction_schema = StructType([
    StructField("actionType", StringType(), True),
    StructField("category", StringType(), True),
    StructField("channelID", StringType(), True),
    StructField("channelName", StringType(), True),
    StructField("duration", LongType(), True),
    StructField("firstEvent", TimestampType(), True),
    StructField("inventoryID", StringType(), True),
    StructField("runtime", LongType(), True),
    StructField("title", StringType(), True),
    StructField("userID", StringType(), True)])

class SparkParquetIO(object):
    """IO object for spark platform under parquet file format
    """

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("Python Spark SQL") \
            .config("spark.sql.warehouse.dir", WAREHOUSE_DIR) \
            .getOrCreate()

    def _creat_empty_dataframe(self):
        sqlContext = SQLContext(self.spark.sparkContext)
        return sqlContext.createDataFrame([],interaction_schema)

    def _load_parquet(self, path_list):
        """Load parquet files into dataframes

        Args:
            path_list: a list of names for parquet file
            exampel:
            ["2017-01", "2107-08"]
        """
        data_frames = [self.spark.read.parquet(PATH.format(x)) for x in path_list]
        return reduce(lambda a, b: a.union(b), data_frames)

    def get_interactions(self, dt_start, dt_end, cols='*'):
        """Fetch interactions from parquet

        Parquet files are organized in to monthly file structure,
        so first find out all the month between and load

        Args:
            dt_start: datetime object stating the start of interactions
            dt_end: datetime object stating the end of interactions
            cols: a list of field names, default to all

        Returns:
            a data frame with all interactions within date range
        """
        if dt_start > UCI_END_DATE or dt_end < UCI_START_DATE:
            return self._creat_empty_dataframe()
        dt_start = max(UCI_START_DATE, dt_start)
        dt_end = min(UCI_END_DATE, dt_end)
        start_str = dt_start.strftime('%Y-%m')
        end_str = dt_end.strftime('%Y-%m')
        # by adding 27 days ensure to get every month in between
        date_in_between = [dt_start + timedelta(days=x*27)
                for x in range(((dt_end-dt_start).days/27) +1)] + [dt_end]
        path_list = list(set([x.strftime('%Y-%m') for x in date_in_between]))
        df = self._load_parquet(path_list)
        df = df.filter(
                (dt_start < df.firstEvent) & (df.firstEvent <dt_end)
            )\
            .select(cols)
        return df

    def get_filtered_interactions(self, dt_start, dt_end, query, cols='*'):
        """Fetch interactions with with query

        Args:
            dt_start: datetime object stating the start of interactions
            dt_end: datetime object stating the end of interactions
            query: SQL expression which is used for filter
            cols: a list of field names, default to all

        Returns:
            a data frame with all interactions that satisfy the query
        """
        df = self.get_interactions(dt_start, dt_end, cols)
        return df.filter(query)

    def get_daily_interactions(self, dt, cols='*'):
        """Fetch interactions for a day
            If 2017-05-05 is sent in, interaction between 05-05->05-06 should be returned
        Args:
            dt: datetime

        Returns:
            a data frame with all interactions that satisfy the query
        """
        return self.get_interactions(dt, dt+timedelta(days=1), cols=cols)

    def get_weekly_interactions(self, dt, cols='*'):
        """Fetch interactions for a week"""
        return self.get_interactions(dt-timedelta(days=6), dt+timedelta(days=1), cols=cols)



if __name__ == '__main__':
    d1 = datetime(2016, 11, 4)
    d2 = datetime(2016, 11, 9)
    cols = ['channelID']
    spark_io = SparkParquetIO()
    interactions = spark_io.get_interactions(d1, d2)
    print interactions.count()
