from pymongo import MongoClient
WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


"""
def build_user_package():
    d = {}
    with open('/media/vionlabs/logs/telenor_log/TV/user_package/CHANNELPACKAGE_per_MSMW_SUB_list_2017-06-17.txt', 'r') as f:
        for line in f:
            user_id, package = line.split(';')
            if user_id in d:
                d[user_id].append(package)
            else:
                d[user_id] = [package]
    return d

def get_package(user_dict):
    package_info = dict()
    for user in user_dict:
        if any(['T3' in x for x in user_dict[user]]):
            package_info[user] = {'basicPackage': 'T3', 'additionalPackage': []}
        elif any(['T2' in x for x in user_dict[user]]):
            package_info[user] = {'basicPackage': 'T2', 'additionalPackage': []}
        else:
            package_info[user] = {'basicPackage': 'T1', 'additionalPackage': []}
        if any([('VIASAT' in x and 'VIASAT_B' not in x) for x in user_dict[user]]):
            package_info[user]['additionalPackage'].append('VIASAT')
        if any(['CMORE' in x for x in user_dict[user]]):
            package_info[user]['additionalPackage'].append('CMORE')
    return  package_info

def user_to_basic_package(user_id):
    pass
"""

def title_count_for_genre(genre):
    col = MongoClient().telenor.epg
    return col.find({"category.value": genre}).count()

def total_title_count():
    col = MongoClient().telenor.epg
    return col.find({}).count()

def float_devision(numerator, denominator):
    return float(numerator) / denominator if denominator != 0 else 0

def normalize(list_tuples, value_sum):
    return [(x[0], float_devision(x[1], value_sum)) for x in list_tuples]

def ucis_completion_ratio(ucis):
    return len([x for x in ucis if finished(x['duration'], x['runtime'])]) / float(len(ucis))

def duration_mapper(duration):
    return "{0} hours".format(str(duration / 3600))

def finished(duration, runtime):
    # NOTE completion is defined as 90% finished
    return duration > 0.9 * runtime
def user_viewtime(ucis):
    user_num = len(set([x['userID'] for x in ucis]))
    return sum([x['duration'] for x in ucis]) / float(user_num)

counter_mapper_doc = {
    -1: '5',
    0: '10',
    1: '20',
    2: '30',
    3: '40',
    4: '50',
    5: '60',
    6: 'more than 60'
}

viewtime_mapper_doc = {
    -1: '1 h',
    0: '2 h',
    1: '4 h',
    2: '6 h',
    3: '8 h',
    4: '10 h',
    5: '12 h',
    6: '14 h',
    7: '16 h',
    8: '18 h',
    9: '20 h',
    10: 'more than 20 h'
}

def viewtime_mapper(viewtime):
    if viewtime < 3600:
        return -1
    elif viewtime  > 7200 * 10:
        return 10
    else:
        return viewtime / 7200

def program_count_mapper(x):
    if x < 5:
        return -1
    elif x >= 60:
        return 6
    else:
        return x / 10
