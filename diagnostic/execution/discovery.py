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

def popular_uncompleted_channels():
    res = r.db('telenortv_insight_api').table('channel_overview')\
        .filter(
            (r.row['content-completion'] < 0.32) & (r.row['channel-total-started-views'] > 100000)
        )\
        .order_by('content-completion')\
        .run()
    table = Table()
    table.title = "popular low completion channels"
    table_rows = [
        TableRow(channelID=x['channelID'], viewCount=x['channel-total-started-views'] ,completion=x['content-completion'])
        for x in res]
    table.rows = table_rows
    return table.as_dict()

def run_gerne_discovery(dt):
    genres = ['Barn/ungdom', 'Dramaserie', 'Komediserie', 'Krim/thrillerserie', 'Fotboll']
    res = []
    for genre in genres:
        genre_ucis = month_ucis.filter(month_ucis.category==genre)
        genre_top_programs = top_programs_by_view_count(genre_ucis)
        table = Table()
        table.title = "top programs for {}".format(genre)
        table_rows = [
            TableRow(programTitle=x['title'], channelName=x['channelName'] ,viewCount=x['count'])
            for x in genre_top_programs]
        table.rows = table_rows
        res.append(table.as_dict())
    return res

def channel_completion(week_ucis):
    channel_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'channelID')
    table = Table()
    table.title = "top channels by content completion"
    table_rows = [
        TableRow(channelID=x['channelID'], completion=x['contentCompletion'])
        for x in channel_by_completion_ratio]
    table.rows = table_rows
    return table.as_dict()

def channel_user_viewtime(week_ucis):
    channel_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'channelID')
    table = Table()
    table.title = "top channels by user viewing time"
    table.subtitle = "top channels by user viewing time"
    table_rows = [
        TableRow(channelID=x['channelID'], viewtimeMinutes=x['viewtime'])
        for x in channel_by_user_viewtime]
    table.rows = table_rows
    return table.as_dict()

def genre_completion(week_ucis):
    genre_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'category')
    table = Table()
    table.title = "top genres by content completion"
    table_rows = [
        TableRow(genre=x['category'], completion=x['contentCompletion'])
        for x in genre_by_completion_ratio]
    table.rows = table_rows
    return table.as_dict()

def genre_user_viewtime(week_ucis):
    genre_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'category')
    table = Table()
    table.title = "top genres by content completion"
    table.subtitle = "top genres by user viewing time"
    table_rows = [
        TableRow(genre=x['category'], viewtimeMinutes=x['viewtime'])
        for x in genre_by_user_viewtime]
    table.rows = table_rows
    return table.as_dict()


if __name__ == '__main__':
    dt = datetime(2017, 6, 27)
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(dt)
    # month_ucis = spark_io.get_interactions(dt-timedelta(days=30), dt)
    print genre_user_viewtime(week_ucis)
