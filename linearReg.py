__author__ = 'Alex Frank'
from scipy import stats
import numpy as np
import json


def main():
    timestamps = []
    bottom_norms = []
    top_norms = []

    # expects norms.dat in same directory. Can be changed to be a command-line arg
    f = open('norms.dat', 'r')
    for line in f:
        words = line.split(' ')
        timestamps.append(words[0][4:24])
        bottom_norms.append(words[1])
        top_norms.append(words[2])

    slope, intercept, r_value, p_value, std_err = stats.linregress(np.asarray(timestamps, float), np.asarray(bottom_norms, float))

    bottom_result = {
        'slope': slope,
        'intercept': intercept,
        'start_time': timestamps[0]
    }

    slope, intercept, r_value, p_value, std_err = stats.linregress(np.asarray(timestamps, float), np.asarray(top_norms, float))

    top_result = {
        'slope': slope,
        'intercept': intercept,
        'start_time': timestamps[0]
    }

    result = {
        "bottom": bottom_result,
        "top": top_result
    }

    return json.dumps(result)