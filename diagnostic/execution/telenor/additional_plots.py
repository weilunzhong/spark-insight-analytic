from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.utils import normalize
from diagnostic.execution.utils import channel_ids
from datetime import datetime, timedelta
from componentmodels.models import TableRow, Table
from pymongo import MongoClient
from functional import seq
import rethinkdb as r
import pytz
import os


RDB_HOST = os.getenv('RDB_HOST', 'localhost')
RDB_PORT = os.getenv('RDB_PORT', 28015)
r.connect(host=RDB_HOST, port=RDB_PORT).repl()
col = MongoClient().telenor.metadata
def vod_genre(inventory_id):
    res = col.find_one({'_id': inventory_id})
    if res:
        return res.get('genre')
    else:
        return

def channleID_to_channelName():
    channel_dict = dict()
    with open('/home/vionlabs/Downloads/fwddata/channel_to_epg_with_id.csv', 'r') as f:
        for line in f:
            data = line.strip().split(';')
            channel_dict[data[3]] = data[1]
    return channel_dict

def channel_by_viewtime_high_low(week_ucis):
    channel_dict = channleID_to_channelName()
    channel_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'channelID', row_limit=200)
    res = channel_by_user_viewtime[:20]
    return {"data": [(channel_dict[x[0]], x[1]) for x in res],
            "id": "top-channel-by-user-viewtime"}

def genre_by_hour_of_day(week_ucis, genre):
    genre_ucis = week_ucis.filter(week_ucis.category==genre)
    genre_hour_of_day = view_count_by_hour_of_day(genre_ucis)
    genre_sum = sum([x[1] for x in genre_hour_of_day])
    return {"data": normalize(genre_hour_of_day, genre_sum),
            "id": "{}-hour-of-day".format(genre.replace('/', '-'))}

def top_genre_vod(ucis, vod):
    vod_with_views = ucis\
        .filter(ucis.actionType==vod)\
        .groupBy(ucis.inventoryID)\
        .count()\
        .collect()
    res = seq(vod_with_views)\
        .map(lambda x: (vod_genre(x['inventoryID']), x['count']))\
        .filter(lambda (genre, count): genre)\
        .group_by(lambda (genre, count): genre)\
        .map(lambda (genre, g_list): (genre, sum([x[1] for x in g_list])))\
        .order_by(lambda (g, c): -c)\
        .to_list()
    vod_sum = sum([x[1] for x in res])
    return {"data": normalize(res, vod_sum)[:10],
            "id": "{}-genre-by-views".format(vod)}

def package_by_action(week_ucis):
    users_actions = week_ucis\
        .groupBy(week_ucis.actionType, week_ucis.userID)\
        .count()\
        .groupBy(week_ucis.actionType)\
        .collect()
    print users_actions[0]

def run_plots(week_ucis, month_ucis):
    res = [
        # channel_by_viewtime_high_low(week_ucis),
        # genre_by_hour_of_day(week_ucis, 'Barn/ungdom'),
        # genre_by_hour_of_day(week_ucis, 'Nyhetsmagazin'),
        top_genre_vod(month_ucis, 'svod'),
        top_genre_vod(month_ucis, 'tvod'),
        # package_by_action(week_ucis),
    ]
    print res
    r.db('telenortv_insight_api').table('extra_plots').insert(res, conflict='replace').run()
    print "#"*10


if __name__ == '__main__':
    dt = datetime(2017, 6, 27)
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(dt)
    month_ucis = spark_io.get_interactions(dt-timedelta(days=30), dt)
    run_plots(week_ucis, month_ucis)
