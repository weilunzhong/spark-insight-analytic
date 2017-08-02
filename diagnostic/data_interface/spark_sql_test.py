from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import desc, array_contains
from datetime import datetime


interaction_schema = StructType([
    StructField("actionType", StringType(), True),
    StructField("category", StringType(), True),
    StructField("channelID", StringType(), True),
    StructField("channelName", TimestampType(), True),
    StructField("duration", LongType(), True),
    StructField("firstEvent", DateType(), True),
    StructField("inventoryID", StringType(), True),
    StructField("runtime", LongType(), True),
    StructField("tilte", StringType(), True),
    StructField("userID", StringType(), True)])

subscription_schema = StructType([
    StructField("userID", StringType(), True),
    StructField("packageName", StringType(), True)])

def avg_user_viewtime(ucis):
    user_num = len(set([x['userID'] for x in ucis]))
    return sum([x['duration'] for x in ucis]) / float(user_num), user_num

def ucis_completion_ratio(ucis):
    return len([x for x in ucis if finished(x['duration'], x['runtime'])]) / float(len(ucis))

def finished(duration, runtime):
    return duration > 0.8 * runtime

def user_action_aggregator(ucis):
    actions = set([x['actionType'] for x in ucis])
    actions = set([action_mapper(x) for x in list(actions)])
    return (',').join(sorted(list(actions)))

def duration_mapper(duration):
    if duration < 300:
        return "5 mins"
    elif duration < 600:
        return "10 mins"
    elif duration < 1200:
        return "20 mins"
    elif duration < 3600:
        return "1 hour"
    elif duration < 7200:
        return "2 hours"
    elif duration < 10800:
        return "3 hours"
    elif duration < 14400:
        return "4 hours"
    elif duration < 18000:
        return "5 hours"
    elif duration < 21600:
        return "6 hours"
    elif duration < 25200:
        return "7 hours"
    elif duration < 28800:
        return "8 hours"
    elif duration < 32400:
        return "9 hours"
    elif duration < 36000:
        return "10 hours"
    else:
        return "more than 10 hours"

def program_count_mapper(count):
    if count < 1:
        return 'nothing'
    elif count < 2:
        return 'less than 2'
    elif count < 5:
        return 'less than 5'
    elif count < 10:
        return 'less than 10'
    elif count < 20:
        return 'less than 20'
    elif count < 30:
        return 'less than 30'
    elif count < 40:
        return 'less than 40'
    else:
        return 'more than 40'

def action_mapper(action):
    if "live.catchup" in action:
        return "catchup"
    elif "live.resume" in action:
        return "time shift"
    elif "live.startover" in action:
        return "startover"
    elif "lpvr" in action:
        return "lvpr"
    elif "live.ongoing" in action:
        return "live"
    elif "svod" in action:
        return "svod"
    elif "tvod" in action:
        return "tvod"
    else:
        print action
        return "other"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


def get_user_interactions(df, user_id):
    ex_user = df.rdd\
            .filter(lambda x: x['userID']=='32363')\
            .collect()
    return ex_user

def user_number(df):
    user_num = df.groupBy(df.userID).count().count()

    print "user number: ", user_num

def user_daily_viewing_time(df):
    # user daily consumption in buckets
    user_finished_content = df.rdd\
        .groupBy(lambda x: x['userID'])\
        .map(lambda (user_id, interactions):
            (user_id, duration_mapper(sum([x['duration'] for x in list(interactions)])))
        )\
        .groupBy(lambda (user_id, category): category)\
        .map(lambda (category, users): (category, len(users)))\
        .collect()
    print user_finished_content

def finished_program_by_user(df):
    finished_program = df.rdd\
        .filter(lambda x: finished(x['duration'], x['runtime']))\
        .groupBy(lambda x: x['userID'])\
        .map(lambda (user_id, user_interactions):
                program_count_mapper(len(user_interactions))
        )\
        .groupBy(lambda x: x)\
        .map(lambda (category, users): (category, len(users)))\
        .collect()
    print finished_program


def viewing_time(df):
    duration_bucket = df.rdd\
        .map(lambda x: duration_mapper(x['duration']))\
        .groupBy(lambda x: x)\
        .map(lambda(x, y): (x, len(y)))\
        .collect()
        # .takeOrdered(10, lambda (x, y): y)

    print duration_bucket

def finished_interaction_by_user(df):
    finished_duration_bucket = df.rdd\
        .filter(lambda x: finished(x['duration'], x['runtime']))\
        .map(lambda x: duration_mapper(x['duration']))\
        .groupBy(lambda x: x)\
        .map(lambda(x, y): (x, len(y)))\
        .takeOrdered(10, lambda (x, y): y)
    print finished_duration_bucket

def average_user_interaction_time(df):
    user_viewtime = df.rdd\
        .groupBy(lambda x: x['userID'])\
        .map(lambda (user, ucis): sum([x['duration'] for x in ucis]))\
        .collect()
    print float(sum(user_viewtime)) / len(user_viewtime)

def action_grouping(df):
    action_group = df.rdd\
        .map(lambda x: action_mapper(x['actionType']))\
        .groupBy(lambda x: x)\
        .map(lambda (action, ucis): (action, len(ucis)))\
        .collect()
    print action_group

def user_action_grouping(df):
    user_action = df.rdd\
        .groupBy(lambda x: x['userID'])\
        .map(lambda (user, ucis): user_action_aggregator(ucis))\
        .groupBy(lambda x: x)\
        .map(lambda (actions, users): (actions, len(users)))\
        .collect()
    print user_action

def user_completed_programs_distribution(df):
    user_completed_programs = df.rdd\
        .filter(lambda x: finished(x['duration'], x['runtime']))\
        .groupBy(lambda x: x['userID'])\
        .map(lambda (user, ucis): (user, len(ucis)))\

def ucis_by_hour_of_day(df):
    hour_of_day = df.rdd\
        .groupBy(lambda x: x['firstEvent'].hour)\
        .map(lambda (hour, ucis): (hour, len(ucis)))\
        .sortBy(lambda(hour, count): hour)\
        .collect()
    print hour_of_day

def ucis_day_of_week(df):
    hour_of_day = df.rdd\
        .groupBy(lambda x: x['firstEvent'].weekday())\
        .map(lambda (weekday, ucis): (weekday, len(ucis)))\
        .sortBy(lambda(weekday, count):weekday)\
        .collect()
    print hour_of_day

def channel_interaction_count(df):
    channel_interactions = df.rdd\
        .groupBy(lambda x: x['channelID'])\
        .map(lambda (channel, ucis): (channel, len(ucis)))\
        .sortBy(lambda (channel, num): num)\
        .collect()
    print channel_interactions

def channel_completion_ratio(df):
    channel_completion = df.rdd\
        .groupBy(lambda x: x['channelID'])\
        .map(lambda (channel, ucis): (channel, ucis_completion_ratio(ucis), len(ucis)))\
        .sortBy(lambda (channel, completion, num): completion)\
        .collect()
    print channel_completion

def channel_by_user_viewtime(df):
    channel_viewtime = df.rdd\
        .groupBy(lambda x: x['channelID'])\
        .map(lambda (channel, ucis): (channel, avg_user_viewtime(ucis)))\
        .filter(lambda(channel, viewtime): viewtime[1] > 100)\
        .sortBy(lambda (channel, viewtime): viewtime[0])\
        .collect()
    print channel_viewtime

def genre_by_user_viewtime(df):
    genre_viewtime = df.rdd\
        .groupBy(lambda x: x['category'])\
        .map(lambda (channel, ucis): (channel, avg_user_viewtime(ucis)))\
        .filter(lambda(channel, viewtime): viewtime[1] > 100)\
        .sortBy(lambda (channel, viewtime): viewtime[0])\
        .collect()
    print len(genre_viewtime)
    print genre_viewtime

def genre_completion_ratio(df):
    genre_completion = df.rdd\
        .groupBy(lambda x: x['category'])\
        .map(lambda (channel, ucis): (channel, ucis_completion_ratio(ucis), len(ucis)))\
        .sortBy(lambda (channel, completion, num): completion)\
        .collect()
    print genre_completion

def top_programs_given_channel(df, channel_id):
    top_programs = df.rdd\
        .filter(lambda x: x['channelID'] == channel_id)\
        .groupBy(lambda x: x['title'])\
        .map(lambda (title, ucis): (title, len(ucis)))\
        .takeOrdered(10, lambda(title, num): -num)
    print top_programs

def top_programs_given_genre(df, genre):
    top_programs = df.rdd\
        .filter(lambda x: x['category'] == genre)\
        .groupBy(lambda x: x['title'])\
        .map(lambda (title, ucis): (title, len(ucis)))\
        .takeOrdered(10, lambda(title, num): -num)
    print top_programs

def top_programs_by_views(df):
    top_programs = df.rdd\
            .filter(lambda x: 'CMORE' in x['channelName'])\
            .groupBy(lambda x: x['title'])\
            .map(lambda (title, ucis):
                    (list(ucis)[0]['title'], list(ucis)[0]['inventoryID'], len(ucis))
            )\
            .sortBy(lambda (title, channel ,num): -num)\
            .collect()
    print top_programs[:20]

def get_unique_user_from_df(df):
    return df.rdd.groupBy(lambda x: x['userID']).map(lambda (x, ucis): x).collect()

def unmatched_epg_by_channel(df):
    unmatched = df.filter(df.runtime.isNull())\
        .rdd.groupBy(lambda x: x['channelName'])\
        .map(lambda (name, ucis): (name, len(ucis)))\
        .sortBy(lambda (name, count): -count)\
        .collect()
    print unmatched

if __name__ == '__main__':
    # df = spark.read.parquet('/media/vionlabs/logs/telenor_log/parquet/2017-05-05.parquet')
    df = spark.read.parquet('/media/vionlabs/logs/telenor_log/parquet/2017-01.parquet')
    df = df\
        .filter(
            (datetime(2017, 1, 25) < df.firstEvent) & (df.firstEvent < datetime(2017, 1, 31))
        )\
        .select('userID')\
        .distinct()\
        .rdd.map(lambda r: r[0])\
        .count()
    print category
    # user_number(df)
