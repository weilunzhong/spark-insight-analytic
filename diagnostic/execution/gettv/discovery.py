from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.calculation import *
from diagnostic.calculation.utils import normalize
from diagnostic.execution.gettv.all_titles import all_titles
from datetime import datetime, timedelta
from componentmodels.models import TableRow, Table
import rethinkdb as r
import pytz
import os


RDB_HOST = os.getenv('RDB_HOST', 'localhost')
RDB_PORT = os.getenv('RDB_PORT', 28015)
r.connect(host=RDB_HOST, port=RDB_PORT).repl()

def most_popular_contents():
    res = r.db('gettv_insight_api').table('content_overview')\
        .order_by('content-total-started-views')\
        .run()
    table = Table()
    table.title = "all time most popular contents"
    def row_dict(x):
        return {
            "content": {
                "inventoryID": all_titles[x['title']],
                "names": [{'value': x['title']}]
            },
            "values": [{
                "name": "view count",
                "data": x['content-total-started-views']
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in reversed(res)][:10]
    table.rows = table_rows
    return table.as_ui_dict()


def popular_uncompleted_contents():
    res = r.db('gettv_insight_api').table('content_overview')\
        .filter(
            (r.row['content-completion'] < 0.3) & (r.row['content-total-started-views'] > 5000) & (r.row['content-completion'] > 0.01)
        )\
        .order_by('content-completion')\
        .run()
    table = Table()
    table.title = "popular low completion contents"
    def row_dict(x):
        return {
            "content": {
                "inventoryID": all_titles[x['title']],
                "names": [{'value': x['title']}]
            },
            "values": [{
                "name": "view count",
                "data": x['content-total-started-views']
            },
            {
                "name": "content completion",
                "unit": "percentage",
                "data": x['content-completion']
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in res][:10]
    table.rows = table_rows
    return table.as_ui_dict()

def unpopular_completed_contents():
    res = r.db('gettv_insight_api').table('content_overview')\
        .filter(
            (r.row['content-completion'] > 0.8)
        )\
        .filter(
            (r.row['content-total-started-views'] < 5000)
        )\
        .order_by('content-completion')\
        .run()
    table = Table()
    table.title = "unpopular high completion contents"
    def row_dict(x):
        return {
            "content": {
                "inventoryID": all_titles[x['title']],
                "names": [{'value': x['title']}]
            },
            "values": [{
                "name": "view count",
                "data": x['content-total-started-views']
            },
            {
                "name": "content completion",
                "unit": "percentage",
                "data": x['content-completion']
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in res][:10]
    table.rows = table_rows
    return table.as_ui_dict(exclude_version=True)


def run_gerne_discovery(month_ucis):
    genres = ['drama', 'komedie', 'barn']
    res = []
    for genre in genres:
        genre_ucis = month_ucis.filter(month_ucis.category==genre)
        genre_top_programs = top_programs_by_view_count(genre_ucis)
        table = Table()
        table.title = "top programs for {} last month".format(genre)
        def row_dict(x):
            return {
                "content": {
                    "names": [{"value": x['title']}]
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

def content_completion(week_ucis):
    content_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'title')
    table = Table()
    table.title = "top contents by content completion"
    def row_dict(x):
        return {
            "content": {
                "names": [{"value": x[0]}]
            },
            "values": [{
                "name": "content completion",
                "unit": "percentage",
                "data": x[1]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in content_by_completion_ratio]
    table.rows = table_rows
    return table.as_ui_dict()

def content_user_viewtime(week_ucis):
    content_by_user_viewtime = top_tag_by_user_viewtime(week_ucis, 'title')
    table = Table()
    table.title = "top contents by user viewing time"
    table.subtitle = "top contents by user viewing time"
    def row_dict(x):
        return {
            "content": {
                "names": [{"value": x[0]}]
            },
            "values": [{
                "name": "viewing time",
                "unit": "minutes",
                "data": x[1]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in content_by_user_viewtime]
    table.rows = table_rows
    return table.as_ui_dict()

def genre_completion(week_ucis):
    genre_by_completion_ratio = top_tag_by_completion_ratio(week_ucis, 'category')
    table = Table()
    table.title = "top genres by content completion"
    def row_dict(x):
        return {
            "genre": {"names": [{"value": x[0]}]},
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
    table.title = "top genres by user viewing time"
    table.subtitle = "top genres by user viewing time"
    def row_dict(x):
        return {
            "genre": {"names": [{"value": x[0]}]},
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
            "content": {"names": [{"value":x[0]}]},
            "values": [{
                "name": "view count",
                "data": x[3]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in midnight_top_programs]
    table.rows = table_rows
    return table.as_ui_dict()

def all_time_top_programs():
    spark_io = SparkParquetIO()
    ucis = spark_io.get_all_interactions()
    top_programs = top_programs_by_viewtime(ucis, row_limit=20)
    table = Table()
    table.title = "all time top programs by viewing time"
    table.subtitle = "programs most watched"
    def row_dict(x):
        return {
            "content": {"names": [{"value":x[0]}]},
            "values": [{
                "name": "minutes",
                "data": x[2]
            }]
        }
    table_rows = [TableRow(**row_dict(x)) for x in top_programs]
    table.rows = table_rows
    return table.as_ui_dict()

def run_discovery(week_ucis, month_ucis):
    res = [
        all_time_top_programs(),
        popular_uncompleted_contents(),
        unpopular_completed_contents(),
        midnight_favorites(week_ucis),
        genre_user_viewtime(week_ucis),
        genre_completion(week_ucis),
        content_completion(week_ucis),
        content_user_viewtime(week_ucis),
        most_popular_contents(),
    ]
    res = res + run_gerne_discovery(month_ucis)
    print res
    r.db('gettv_insight_api').table('discovery').insert(res, conflict='replace').run()
    print "#"*10


if __name__ == '__main__':
    dt = datetime(2017, 8, 30)
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_weekly_interactions(dt)
    month_ucis = spark_io.get_interactions(dt-timedelta(days=30), dt)
    run_discovery(week_ucis, month_ucis)
