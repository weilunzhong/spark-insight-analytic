from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.utils import total_title_count, title_count_for_genre
from diagnostic.calculation.calculation import *
from diagnostic.calculation.utils import normalize
from diagnostic.execution.utils import channel_ids
from datetime import datetime, timedelta
import rethinkdb as r
import pytz
import os


RDB_HOST = os.getenv('RDB_HOST', 'localhost')
RDB_PORT = os.getenv('RDB_PORT', 28015)
r.connect(host=RDB_HOST, port=RDB_PORT).repl()

def library_overiew(ucis):
    channel_num = len(channel_ids)
    genre_num = genre_number(ucis)
    print channel_num, genre_num
    res = [
        {"channel-number": channel_num, "id": "channel-number"},
        {"genre-number": genre_num, "id": "genre-number"}
    ]
    r.db('telenortv_insight_api')\
        .table('overview')\
        .insert(res, conflict='replace').run()

if __name__ == '__main__':
    spark_io = SparkParquetIO()
    ucis = spark_io.get_all_interactions(cols=['category'])
    library_overiew(ucis)


