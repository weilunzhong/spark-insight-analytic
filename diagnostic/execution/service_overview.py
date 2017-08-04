from diagnostic.data_interface.input_data import SparkParquetIO
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

def service_overview(dt):
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(dt)
    channel_by_completion_ratio = 
    # total_views = week_ucis.count()
    # total_viewtime = week_ucis.groupBy().sum('duration').collect()[0]['sum(duration)']
    # channel_by_views = normalize(
    #     top_tag_by_view_count(week_ucis, 'channelID'), total_views, 'count')
    # channel_by_viewtime = normalize(
    #     top_tag_by_viewing_time(week_ucis, 'channelID'), total_viewtime, 'viewtime')
    # genre_by_views = normalize(
    #     top_tag_by_view_count(week_ucis, 'category'), total_views, 'count')
    # genre_by_viewtime = normalize(
    #     top_tag_by_viewing_time(week_ucis, 'category'), total_viewtime, 'viewtime')
    # user_complete_view = user_by_complete_views(week_ucis)
    # user_viewtime = user_by_viewtime(week_ucis, 7)
    # res = {
    #         "channel-by-views": channel_by_views,
    #         "channel-by-viewtime": channel_by_viewtime,
    #         "genre-by-views": genre_by_views,
    #         "genre-by-viewtime": genre_by_viewtime,
    #         "user-by-complete-views": user_complete_view,
    #         "user-by-viewtime": user_viewtime
    # }
    # print res



if __name__ == '__main__':
    dt = datetime(2017, 6, 27)
    service_overview(dt)

