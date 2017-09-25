from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.user import *
from diagnostic.calculation.utils import normalize
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
        view_count_by_hour_of_day(week_ucis), total_views)
    day_of_week = normalize(
        view_count_by_day_of_week(week_ucis), total_views)
    device_overview = normalize(
        top_tag_by_view_count(week_ucis, 'device'), total_views)
    device_conpletion_ratio = top_tag_by_completion_ratio(week_ucis, 'device')
    location_overview = normalize(
        top_tag_by_view_count(week_ucis, 'city'), total_views)
    ###################
    # this frist one should be wraped with genre title count as well
    genre_by_views = normalize(
        top_tag_by_view_count(week_ucis, 'category', row_limit= 20), total_views)
    ###################
    genre_by_viewtime = normalize(
        top_tag_by_total_viewtime(week_ucis, 'category', row_limit=20), total_viewtime)
    genre_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'category', row_limit=20)
    genre_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'category', row_limit=20)
    # about users
    user_complete_view = user_by_complete_views(week_ucis)
    user_viewtime = user_by_viewtime(week_ucis, 7)
    action_type_overview = top_tag_by_total_viewtime(week_ucis, 'actionType')
    action_type_overview = normalize(
        action_type_overview, sum([x[1] for x in action_type_overview])
    )
    res = [
        {"id": "hour-of-day", "data": hour_of_day},
        {"id": "day-of-week", "data": day_of_week},
        {"id": "genre-by-started-views", "data": genre_by_views},
        {"id": "genre-by-viewtime", "data": genre_by_viewtime},
        {"id": "genre-by-user-viewtime", "data": genre_by_user_viewtime},
        {"id": "genre-by-completion-ratio", "data": genre_by_completion_ratio},
        {"id": "device-by-started-views", "data": device_overview},
        {"id": "device-by-completion-ratio", "data": device_conpletion_ratio},
        {"id": "location-by-started-views", "data": location_overview},
        {"id": "genre-by-completion-ratio", "data": genre_by_completion_ratio},
        {"id": "user-by-complete-views", "data": user_complete_view},
        {"id": "user-by-viewtime", "data": user_viewtime},
        {"id": "action-type-overview", "data": action_type_overview}
    ]

    for x in res:
        print x
    r.db('gettv_insight_api').table('overview').insert(res, conflict='replace').run()



if __name__ == '__main__':
    dt = datetime(2017, 8, 30)
    service_overview(dt)

