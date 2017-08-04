
WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

def float_devision(numerator, denominator):
    return float(numerator) / denominator if denominator != 0 else 0

def normalize(list_dicts, value_sum, field_name):
    def change_value(x):
        x['percentage'] = x.pop(field_name) / float(value_sum)
        return x
    return [change_value(x) for x in list_dicts]

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
    -1: 'less than 5',
    0: 'less than 10',
    1: 'less than 20',
    2: 'less than 30',
    3: 'less than 40',
    4: 'less than 50',
    5: 'less than 60',
    6: 'more than 60'
}

viewtime_mapper_doc = {
    -1: 'less than 1 hour',
    0: 'less than 2 hours',
    1: 'less than 4 hours',
    2: 'less than 6 hours',
    3: 'less than 8 hours',
    4: 'less than 10 hours',
    5: 'less than 12 hours',
    6: 'less than 14 hours',
    7: 'less than 16 hours',
    8: 'less than 18 hours',
    9: 'less than 20 hours',
    10: 'more than 20 hours'
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
