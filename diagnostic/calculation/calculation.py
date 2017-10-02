from diagnostic.data_interface.input_data import SparkParquetIO
from diagnostic.calculation.utils import *
from pyspark.sql import functions as func
from datetime import datetime
from functional import seq


# scala like functional processing package is used

# __all__ is defined for easy explicit import
__all__ = [
    'view_count', 'user_number', 'avg_user_viewtime', 'genre_number',
    'avg_user_view_count', 'avg_finished_program_by_user',
    'completed_views', 'views_with_runtime', 'content_consumption',
    'avg_completion_ratio', 'action_type_view_count',
    'user_hibernation', 'top_programs_by_view_count',
    'top_tag_by_view_count', 'top_tag_by_total_viewtime',
    'view_count_by_hour_of_day', 'view_count_by_day_of_week',
    'top_tag_by_user_viewtime', 'user_by_complete_views',
    'user_by_viewtime', 'top_tag_by_completion_ratio',
    'midnight_favorite_programs', 'top_programs_by_viewtime'
]

"""
All the calculations(aggrgation) is defined here, in a functional programing fashion
This is for easier transferablity if we need to convert to Scala
Genernal role for calculations in this script:

    1. input data must be a pyspark dataframe, RDD or pyspark streaming should be converted
    2. if possible, do not convert df to RDD, since RDD is way too slow on large datasets
    3. try the udf in pyspark to avoid RDD
    4. return object should be either a number eg: user_num 63
                                      a list eg: [(ubuntu, 12), (windows, 32), ...]
    5. remember to mark the unit in the code!!!
    6. Pls do a select fields first thing, so memory is not overloading
"""

def view_count(df):
    return df.count()

def user_number(df):
    user_num = df.select('userID').distinct().count()
    return user_num

def genre_number(df):
    # category is a bit hard coded, modify later
    return df.filter(df.category.isNotNull())\
        .groupBy(df.category)\
        .count()\
        .count()

def content_consumption(df):
    consumption_seconds = df\
        .select('duration')\
        .agg(func.sum('duration'))\
        .first()['sum(duration)']  # return a list of dataframe
    return consumption_seconds / 60.

def avg_user_viewtime(df):
    # measured in minutes
    user_viewing_time = df\
        .select('duration')\
        .agg(func.sum('duration'))\
        .first()  # return a list of dataframe
    total_viewing_time =  user_viewing_time['sum(duration)']
    return float_devision(total_viewing_time, (user_number(df) * 60))

def avg_user_view_count(df):
    return float_devision(view_count(df), user_number(df))

def avg_finished_program_by_user(df):
    # first filter runtime exists, otherwise finished methods raise ValueError
    finished_program = df\
        .filter(df.runtime.isNotNull())\
        .filter(finished(df.duration, df.runtime))\
        .count()
    return float_devision(finished_program, user_number(df))

def completed_views(df):
    # first filter runtime exists, otherwise finished methods raise ValueError
    return df\
        .select('duration', 'runtime')\
        .filter(df.runtime.isNotNull())\
        .filter(finished(df.duration, df.runtime))\
        .count()

def views_with_runtime(df):
    return df\
        .select('duration', 'runtime')\
        .filter(df.runtime.isNotNull())\
        .count()

def avg_completion_ratio(df):
    # completion ratio is out of all the views with runtime how much is finished
    # finished is defined in utils.py
    completion_titles = df\
        .select('duration', 'runtime')\
        .filter(df.runtime.isNotNull())\
        .filter(finished(df.duration, df.runtime))\
        .count()
    titles_with_runtime = df\
        .select('duration', 'runtime')\
        .filter(df.runtime.isNotNull())\
        .count()
    return float_devision(completion_titles, titles_with_runtime)

def user_by_complete_views(df):
    # group user by their coresponding completed views
    hist = df\
        .filter(df.runtime.isNotNull())\
        .filter(finished(df.duration, df.runtime))\
        .groupBy('userID')\
        .count()\
        .select(func.col('count').alias('userViews'))\
        .groupBy('userViews')\
        .count()\
        .collect()
    program_count_hist = [(program_count_mapper(x['userViews']), x['count']) for x in hist]
    # here using scala like sequence package, for further group and map
    res = seq(program_count_hist)\
        .group_by(lambda(category, count): category)\
        .map(lambda (category, count_list): (category, sum([x[1] for x in count_list])))\
        .order_by(lambda (category, count): category)\
        .map(lambda(category, count):
            (counter_mapper_doc[category], count)
        )\
        .to_list()
    return res

def user_by_viewtime(df, interval):
    # NOTE that interval must be send to ensure the mapper works, so no default value
    hist = df\
        .groupBy('userID')\
        .agg(func.sum('duration'))\
        .select(func.col('sum(duration)').alias('viewtime'))\
        .collect()
    user_viewtime = seq([x['viewtime'] / interval for x in hist])\
        .map(lambda x: viewtime_mapper(x))\
        .group_by(lambda x: x)\
        .map(lambda (category, users): (category, len(users)))\
        .order_by(lambda (category, count): category)\
        .map(lambda (category, count):
                (viewtime_mapper_doc[category], count)
        )\
        .to_list()
    return user_viewtime


"""
Below are few methods that takes in a tag and perform tag aggregations
Here tag can be any categorical field in the input data
eg:
    content title, device, action, etc

return:
    list of top tags, normally with tagID, and aggregated data for that tag
"""
def top_tag_by_view_count(df, tag_name, row_limit=10):
    top_tag = df\
        .select(tag_name)\
        .filter(df[tag_name].isNotNull())\
        .groupBy(tag_name)\
        .count()\
        .orderBy(func.desc('count'))\
        .limit(row_limit)\
        .rdd\
        .collect()
    top_tag = [
        (x[tag_name], x['count'])
        for x in top_tag]
    return top_tag

def top_tag_by_total_viewtime(df, tag_name, row_limit=10):
    # measured in minutes
    top_tag = df\
        .select(tag_name, 'duration')\
        .filter(df[tag_name].isNotNull())\
        .groupBy(tag_name)\
        .agg(func.sum('duration'))\
        .sort(func.desc('sum(duration)'))\
        .limit(row_limit)\
        .rdd.collect()
    top_tag = [
        (x[tag_name], x['sum(duration)'] / 60)
        for x in top_tag]
    return top_tag

def top_tag_by_completion_ratio(df, tag_name, row_limit=10, tag_count_limit=100):
    """Top tags ranked by completion ratio
    Args:
        tag_name
        row_limit: number of returned tags
        tag_count_limit: number of interactions for a tag to be considered
    Return:
        list of tags with parameters
    """
    tag_by_views = df\
        .filter(df[tag_name].isNotNull())\
        .filter(df.runtime.isNotNull())\
        .groupBy(tag_name)\
        .count()\
        .rdd\
        .collectAsMap()
    tag_by_finished_views = df\
        .filter(df[tag_name].isNotNull())\
        .filter(df.runtime.isNotNull())\
        .filter(finished(df.duration, df.runtime))\
        .groupBy(tag_name)\
        .count()\
        .collect()
    res = [
        (
            x[tag_name],
            float_devision(x['count'], tag_by_views[x[tag_name]])
        ) for x in tag_by_finished_views if x['count'] > tag_count_limit
    ]
    return sorted(res, key=lambda x: x[1], reverse=True)[:row_limit]

def top_tag_by_user_viewtime(df, tag_name, row_limit=10):
    top_channel = df\
        .filter(df[tag_name].isNotNull())\
        .groupBy('userID', tag_name)\
        .sum('duration')\
        .groupBy(tag_name)\
        .avg('sum(duration)')\
        .sort(func.desc('avg(sum(duration))'))\
        .limit(row_limit)\
        .rdd.collect()
    top_channel = [
        (x[tag_name], x['avg(sum(duration))'] / 60)
        for x in top_channel]
    return top_channel

def top_programs_by_view_count(df, row_limit=10):
    top_programs = df\
        .filter(df.title.isNotNull())\
        .select('title', 'channelName', 'channelID')\
        .groupBy('channelName', 'title', 'channelID')\
        .count()\
        .orderBy(func.desc('count'))\
        .limit(row_limit)\
        .rdd\
        .collect()
    top_programs = [
        {"title": x['title'], "channelName": x['channelName'],
         "channelID": x['channelID'], "viewCount": x['count']}
        for x in top_programs]
    return top_programs

"""
End of tag aggregation segment
"""

def action_type_view_count(df):
    action_type = df\
        .select('actionType')\
        .groupBy('actionType')\
        .count()\
        .orderBy(func.desc('count'))\
        .rdd.collect()
    action_type = [
        (x['actionType'], x['count'])
        for x in action_type]
    return action_type

"""
def view_count_by_hour_of_day(df):
    hour_of_day = df\
        .groupBy(func.hour('firstEvent').alias('hour'))\
        .count()\
        .sort('hour')\
        .collect()
    hourly_bucket = [(x, 0) for x in range(24)]
    for d in hour_of_day:
        hourly_bucket[d['hour']] = (d['hour'], d['count'])
    return hourly_bucket
"""
# this is nodified for faster aggregation in query time
def view_count_by_hour_of_day(df):
    hour_of_day = df\
        .groupBy(func.hour('firstEvent').alias('hour'))\
        .count()\
        .sort('hour')\
        .collect()
    hourly_bucket = [0 for x in range(24)]
    for d in hour_of_day:
        hourly_bucket[d['hour']] = d['count']
    return hourly_bucket

def view_count_by_day_of_week(df):
    day_of_week = df\
        .groupBy(func.date_format('firstEvent', 'E').alias('weekday'))\
        .count()\
        .collect()
    week_bucket = {x: 0 for x in WEEKDAYS}
    for d in day_of_week:
        week_bucket[d['weekday']] = d['count']
    day_of_week = [(x, week_bucket[x]) for x in WEEKDAYS]
    return day_of_week

def user_hibernation(df, pre_df):
    """User hibernation
    Hibernation is defined :
        out of all the active users in tracking period 1, the ones that left in tracking period 2 are defined as hibernated

    Args:
        df: dataframe contains all interacitons in tracking preiod 2
        pre_df: dataframe contains all interacitons in tracking preiod 1
    Returns:
        hibernation ratio : Float, 0 if no interaction in pre_df
    """
    # df and pre_df should have the same days of interaction in it
    users = df.select('userID').distinct().rdd.map(lambda x: x['userID']).collect()
    pre_users = pre_df.select('userID').distinct().rdd.map(lambda x: x['userID']).collect()
    # here user set operation since list comprehension is too slow
    missing_user = len(set(users + pre_users)) - len(users)
    return float_devision(float(missing_user), len(pre_users))

def midnight_favorite_programs(df):
    """Midnight favorite programs
    Args:
        df: dataframe contains all interacitons
    Returns:
        list of contents that is most popular within midnight(21pm-1am)
    """
    midnight_favorites = df\
        .filter(df.title.isNotNull())\
        .groupBy(df.title, df.channelID, df.channelName, func.hour('firstEvent').alias('hour'))\
        .count()\
        .collect()

    res = seq(midnight_favorites)\
        .filter(lambda x: x['hour'] <1 or x['hour']>21)\
        .group_by(lambda x: x['title'])\
        .map(lambda (title, title_buckets):
            (title, title_buckets[0]['channelName'],
                title_buckets[0]['channelID'], sum([x['count'] for x in title_buckets])))\
        .order_by(lambda (title, name, c_id, count): -count)\
        .to_list()

    return res[:10]

def top_programs_by_viewtime(df, row_limit=10):
    """Most consumed content by aggregated time"""
    top_tag = df\
        .select('title', 'channelID', 'duration')\
        .filter(df['title'].isNotNull())\
        .groupBy('title', 'channelID')\
        .agg(func.sum('duration'))\
        .sort(func.desc('sum(duration)'))\
        .limit(row_limit)\
        .rdd.collect()
    top_tag = [
        (x['title'], x['channelID'], x['sum(duration)'] / 60)
        for x in top_tag]
    return top_tag


if __name__ == '__main__':
    from datetime import timedelta
    timestamp = datetime(2017, 8, 25)
    spark_io = SparkParquetIO()
    week_ucis = spark_io.get_all_interactions()
    print week_ucis.count()
    print week_ucis\
        .filter(week_ucis.duration<180)\
        .count()

