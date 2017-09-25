from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.utils import total_title_count, title_count_for_genre
from diagnostic.calculation.calculation import *
from diagnostic.calculation.user import *
from diagnostic.calculation.utils import normalize
from diagnostic.execution.telenor.utils import channel_ids
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
    # about channels
    channel_by_views = normalize(
        top_tag_by_view_count(week_ucis, 'channelName', row_limit=20), total_views)
    channel_by_viewtime = normalize(
        top_tag_by_total_viewtime(week_ucis, 'channelName', row_limit=20), total_viewtime)
    channel_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'channelName', row_limit=20)
    channel_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'channelName', row_limit=20)
    # about genres
    ###################
    # this frist one should be wraped with genre title count as well
    genre_by_views = normalize(
        top_tag_by_view_count(week_ucis, 'category', row_limit= 20), total_views)
    genre_by_title_count = normalize(
        [(x[0], title_count_for_genre(x[0])) for x in genre_by_views],
        total_title_count()
    )
    genre_overview = {"views": genre_by_views, "titles": genre_by_title_count}
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
    basic_package_by_viewing_time = basic_package_user_viewing_time(week_ucis)
    primeium_package_by_viewing_time = primeium_package_user_viewing_time(week_ucis)
    package_overview = basic_additional_package_overview()
    res = [
        {"id": "user-package-overview", "data":package_overview},
        {"id": "basic-package-user-viewing-time", "data":basic_package_by_viewing_time},
        {"id": "premium-package-user-viewing-time", "data": primeium_package_by_viewing_time},
        {"id": "hour-of-day", "data": hour_of_day},
        {"id": "day-of-week", "data": day_of_week},
        {"id": "channel-by-completion-ratio", "data": channel_by_completion_ratio},
        {"id": "channel-by-views", "data": channel_by_views},
        {"id": "channel-by-viewtime", "data": channel_by_viewtime},
        {"id": "channel-by-user-viewtime", "data": channel_by_user_viewtime},
        {"id": "genre-overview", "data": genre_overview},
        {"id": "genre-by-viewtime", "data": genre_by_viewtime},
        {"id": "genre-by-user-viewtime", "data": genre_by_user_viewtime},
        {"id": "genre-by-completion-ratio", "data": genre_by_completion_ratio},
        {"id": "user-by-complete-views", "data": user_complete_view},
        {"id": "user-by-viewtime", "data": user_viewtime},
        {"id": "action-type-overview", "data": action_type_overview}
    ]

    print res
    r.db('telenortv_insight_api').table('overview').insert(res, conflict='replace').run()



if __name__ == '__main__':
    dt = datetime(2017, 6, 27)
    service_overview(dt)

