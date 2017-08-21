from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.utils import normalize
from diagnostic.execution.utils import channel_ids
from datetime import datetime, timedelta
from componentmodels.models import TableRow, Table
import rethinkdb as r
import pytz
import os


RDB_HOST = os.getenv('RDB_HOST', 'localhost')
RDB_PORT = os.getenv('RDB_PORT', 28015)
r.connect(host=RDB_HOST, port=RDB_PORT).repl()

def channleID_to_channelName():
    channel_dict = dict()
    with open('/home/vionlabs/Downloads/fwddata/channel_to_epg_with_id.csv', 'r') as f:
        for line in f:
            data = line.strip().split(';')
            channel_dict[data[3]] = data[1]
    return channel_dict

def popular_uncompleted_channels():
    channel_dict = channleID_to_channelName()
    res = r.db('telenortv_insight_api').table('channel_overview')\
        .filter(
            (r.row['content-completion'] < 0.32) & (r.row['channel-total-started-views'] > 100000)
        )\
        .order_by('content-completion')\
        .run()
    table = Table()
    table.title = "popular low completion channels"
    def row_dict(x):
        return {
            "content": {
                "ID": x['channelID'],
                "name": channel_dict[x['channelID']]
            },
            "values": [{
                "name": "view count",
                "data": x['channel-total-started-views']
            },
            {
                "name": "content completion",
                "unit": "percentage",
                "data": x['content-completion']
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in res]
    table.rows = table_rows
    return table.as_ui_dict()

def unpopular_completed_channels():
    channel_dict = channleID_to_channelName()
    res = r.db('telenortv_insight_api').table('channel_overview')\
        .filter(
            (r.row['content-completion'] > 0.55)
        )\
        .order_by('content-completion')\
        .run()
    table = Table()
    table.title = "unpopular high completion channels"
    def row_dict(x):
        return {
            "content": {
                "ID": x['channelID'],
                "name": channel_dict[x['channelID']]
            },
            "values": [{
                "name": "view count",
                "data": x['channel-total-started-views']
            },
            {
                "name": "content completion",
                "unit": "percentage",
                "data": x['content-completion']
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in res]
    table.rows = table_rows
    return table.as_ui_dict(exclude_version=True)


def run_gerne_discovery(month_ucis):
    genres = ['Barn/ungdom', 'Dramaserie', 'Komediserie', 'Krim/thrillerserie', 'Fotboll']
    res = []
    for genre in genres:
        genre_ucis = month_ucis.filter(month_ucis.category==genre)
        genre_top_programs = top_programs_by_view_count(genre_ucis)
        table = Table()
        table.title = "top programs for {} last month".format(genre)
        def row_dict(x):
            return {
                "content": {
                    "ID": x['channelID'],
                    "name": x['channelName']
                },
                "values": [{
                    "name": "view count",
                    "data": x['viewCount']
                }]
            }
        table_rows = [TableRow(**row_dict(x)) for x in genre_top_programs]
        table.rows = table_rows
        res.append(table.as_ui_dict())
    return res

def channel_completion(week_ucis):
    channel_dict = channleID_to_channelName()
    channel_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'channelID')
    table = Table()
    table.title = "top channels by content completion"
    def row_dict(x):
        return {
            "content": {
                "ID": x[0],
                "name": channel_dict[x[0]]
            },
            "values": [{
                "name": "content completion",
                "unit": "percentage",
                "data": x[1]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in channel_by_completion_ratio]
    table.rows = table_rows
    return table.as_ui_dict()

def channel_user_viewtime(week_ucis):
    channel_dict = channleID_to_channelName()
    channel_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'channelID')
    table = Table()
    table.title = "top channels by user viewing time"
    table.subtitle = "top channels by user viewing time"
    def row_dict(x):
        return {
            "content": {
                "ID": x[0],
                "name": channel_dict[x[0]]
            },
            "values": [{
                "name": "viewing time",
                "unit": "minutes",
                "data": x[1]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in channel_by_user_viewtime]
    table.rows = table_rows
    return table.as_ui_dict()

def genre_completion(week_ucis):
    genre_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'category')
    table = Table()
    table.title = "top genres by content completion"
    def row_dict(x):
        return {
            "genre": x[0],
            "values": [{
                "name": "content completion",
                "unit": "percentage",
                "data": x[1]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in genre_by_completion_ratio]
    table.rows = table_rows
    return table.as_ui_dict()

def genre_user_viewtime(week_ucis):
    genre_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'category')
    table = Table()
    table.title = "top genres by content completion"
    table.subtitle = "top genres by user viewing time"
    def row_dict(x):
        return {
            "genre": x[0],
            "values": [{
                "name": "viewing time",
                "unit": "minutes",
                "data": x[1]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in genre_by_user_viewtime]
    table.rows = table_rows
    return table.as_ui_dict()

def midnight_favorites(week_ucis):
    midnight_top_programs = midnight_favorite_programs(week_ucis)
    table = Table()
    table.title = "top programs in midnight"
    table.subtitle = "programs most watched between 22:00 to 01:00"
    def row_dict(x):
        return {
            "title": x[0],
            "values": [{
                "name": "view count",
                "data": x[2]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in midnight_top_programs]
    table.rows = table_rows
    return table.as_ui_dict()

def all_time_top_programs():
    spark_io = SparkParquetIO()
    ucis = spark_io.get_all_interactions()
    top_programs = top_tag_by_total_viewtime(ucis,"title", row_limit=20)
    table = Table()
    table.title = "all time top programs by viewing time"
    table.subtitle = "programs most watched"
    def row_dict(x):
        return {
            "title": x[0],
            "values": [{
                "name": "minutes",
                "data": x[1]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in top_programs]
    table.rows = table_rows
    return table.as_ui_dict()

def run_discovery(week_ucis, month_ucis):
    res = [
        all_time_top_programs(),
        popular_uncompleted_channels(),
        unpopular_completed_channels(),
        midnight_favorites(week_ucis),
        genre_user_viewtime(week_ucis),
        genre_completion(week_ucis),
        channel_completion(week_ucis),
        channel_user_viewtime(week_ucis)
    ]
    res = res + run_gerne_discovery(month_ucis)
    print res
    r.db('telenortv_insight_api').table('discovery').insert(res, conflict='replace').run()
    print "#"*10


if __name__ == '__main__':
    dt = datetime(2017, 6, 27)
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(dt)
    month_ucis = spark_io.get_interactions(dt-timedelta(days=30), dt)
    run_discovery(week_ucis, month_ucis)
