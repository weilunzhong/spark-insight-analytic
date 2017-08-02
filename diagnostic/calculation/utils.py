
def float_devision(numerator, denominator):
    return float(numerator) / denominator if denominator != 0 else 0

def normalize(list_dicts, value_sum, field_name):
    def change_value(x):
        x['percentage'] = x.pop(field_name) / float(value_sum)
        return x
    return [change_value(x) for x in list_dicts]


