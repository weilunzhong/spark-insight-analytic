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

def query_channel_weekly(channel_id):
    return r.db('telenortv_insight_api').table('channel_by_week')\
        .filter({'channelID': channel_id}).run()

def channel_overview(dt, week_ucis, channel_id):
    channel_by_week = query_channel_weekly(channel_id)
    channel_data = [(x['started-views'], int(x['viewing-time'] * x['weekly-active-user']))
        for x in channel_by_week]
    total_views = sum([x[0] for x in channel_data])
    # this is already measured in minutes
    total_viewtime = sum([x[1] for x in channel_data])
    channel_hour_of_day = view_count_by_hour_of_day(week_ucis)
    channel_day_of_week = view_count_by_day_of_week(week_ucis)
    weekly_active_user = user_number(week_ucis)
    completion_ratio = avg_completion_ratio(week_ucis)
    user_viewtime = avg_user_viewtime(week_ucis)
    res = {
        "id": channel_id,
        'channelID': channel_id,
        "datetime": dt.replace(tzinfo=pytz.UTC),
        "channel-total-viewing-time": total_viewtime,
        "channel-total-started-views": total_views,
        "hour-of-day": channel_hour_of_day,
        "day-of-week": channel_day_of_week,
        "weekly-active-user": weekly_active_user,
        "content-completion": completion_ratio,
        "viewing-time": user_viewtime
    }
    print "#"*10
    print channel_id
    print res
    # r.db('telenortv_insight_api').table('channel_overview').insert([res]).run()
    print "#"*10



def channel_overview_trigger(dt):
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(dt)
    # for channel_id in channel_ids:
    for channel_id in ['eid25']:
        channel_ucis = channel_overview(
            dt,
            week_ucis.filter(week_ucis.channelID==channel_id),
            channel_id
        )


if __name__ == '__main__':
    dt = datetime(2017, 6, 27)
    channel_overview_trigger(dt)

    # total_views = week_ucis.count()
    # total_viewtime = week_ucis.groupBy().sum('duration').collect()[0]['sum(duration)']
    # channel_by_views = top_channel_by_view_count(week_ucis)
    # channel_by_viewtime = top_channel_by_viewing_time(week_ucis )
