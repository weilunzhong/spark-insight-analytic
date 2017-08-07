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
    total_views = week_ucis.count()
    total_viewtime = week_ucis.groupBy().sum('duration').collect()[0]['sum(duration)'] / 60
    hour_of_day = normalize(
        view_count_by_hour_of_day(week_ucis), total_views, 'count')
    day_of_week = normalize(
        view_count_by_day_of_week(week_ucis), total_views, 'count')
    # about channels
    channel_by_views = normalize(
        top_tag_by_view_count(week_ucis, 'channelID'), total_views, 'count')
    channel_by_viewtime = normalize(
        top_tag_by_total_viewtime(week_ucis, 'channelID'), total_viewtime, 'viewtime')
    channel_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'channelID')
    channel_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'channelID')
    # about genres
    genre_by_views = normalize(
        top_tag_by_view_count(week_ucis, 'category'), total_views, 'count')
    genre_by_viewtime = normalize(
        top_tag_by_total_viewtime(week_ucis, 'category'), total_viewtime, 'viewtime')
    genre_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'category')
    genre_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'category')
    # about users
    user_complete_view = user_by_complete_views(week_ucis)
    user_viewtime = user_by_viewtime(week_ucis, 7)
    res = [
        {"id": "hour-of-day", "data": hour_of_day},
        {"id": "day-of-week", "data": day_of_week},
        {"id": "channel-by-comipletion-ratio", "data": channel_by_completion_ratio},
        {"id": "channel-by-views", "data": channel_by_views},
        {"id": "channel-by-viewtime", "data": channel_by_viewtime},
        {"id": "channel-by-user-viewtime", "data": channel_by_user_viewtime},
        {"id": "genre-by-views", "data": genre_by_views},
        {"id": "genre-by-viewtime", "data": genre_by_viewtime},
        {"id": "genre-by-user-viewtime", "data": genre_by_user_viewtime},
        {"id": "genre-by-comipletion-ratio", "data": genre_by_completion_ratio},
        {"id": "user-by-complete-views", "data": user_complete_view},
        {"id": "user-by-viewtime", "data": user_viewtime}
    ]
    r.db('telenortv_insight_api').table('overview').insert(res).run()



if __name__ == '__main__':
    dt = datetime(2017, 6, 27)
    service_overview(dt)

