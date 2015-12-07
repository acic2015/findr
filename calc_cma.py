__author__ = 'Alby Chaj and Alex Frank'
import numpy as np
import datetime as dt
import time


def calc_cma(filename, size):
    """
    This function calculates the centered moving average (CMA) of the norm values
    for the image data set's top and bottom amplifiers

    Parameters
        filename (str) -- name of dat file containing list of FITS filenames and
            the norm values for the image's top and bottom amplifiers
        size (int) -- desired subset size (between 10-20)

    Returns
        dict
    """
    image_names = []
    timestamps = []
    bottom_norms = []
    top_norms = []

    with open(filename) as dat_file:
        for line in dat_file:
            img_norm_data = line.split()
            image_names.append(img_norm_data[0])
            timestamps.append(img_norm_data[0][4:20])
            bottom_norms.append(float(img_norm_data[1]))
            top_norms.append(float(img_norm_data[2]))

    # Initial smoothing
    window = np.ones(int(size)) / float(size)
    bottom_norm_avgs = np.convolve(bottom_norms, window, 'same')
    top_norm_avgs = np.convolve(top_norms, window, 'same')

    # If the size is even, then we need to smooth the smoothed values
    if size % 2 == 0:
        window = np.ones(2) / float(2)
        bottom_norm_avgs = np.convolve(bottom_norm_avgs, window, 'same')
        top_norm_avgs = np.convolve(top_norm_avgs, window, 'same')

    result = []
    for i in range(len(image_names)):
        result.append({
            "sci_filename": image_names[i],
            "timestamp": timestamps[i],
            "smoothed_btm_norm": bottom_norm_avgs[i],
            "smoothed_top_norm": top_norm_avgs[i]
        })

    return result


def shift_cma(norm_filename, smoothed_sciences):
    """
    This function adjusts the science smoothed norm values based on the norm values
    of the dark images.

    Parameters
        norm_filename (str) -- name of dat file containing list of FITS filenames for dark images and
            the norm values for the image's top and bottom amplifiers
        smoothed_sciences (list) -- the returned result of calc_cma

    Returns
        dict of the form:
        { sci_filename: { "bottom_norm": smoothed and scaled norm value, "top_norm": smoothed and scaled norm value } }
    """
    dark_filenames = []
    dark_timestamps = []
    dark_bottom_norms = []
    dark_top_norms = []

    with open(norm_filename) as dark_norm_file:
        for line in dark_norm_file:
            dark_image_data = line.split()
            dark_filenames.append(dark_image_data[0])
            dark_timestamps.append(dark_image_data[0][4:20])
            dark_bottom_norms.append(float(dark_image_data[1]))
            dark_top_norms.append(float(dark_image_data[2]))

    # convert all timestamps to seconds since epoch
    dark_time_in_seconds = []
    for t in dark_timestamps:
        year = t[0:4]
        month = t[4:6]
        day = t[6:8]
        hours = t[8:10]
        minutes = t[10:12]
        seconds = t[12:14]
        partial_seconds = t[14:20]

        dark_time = dt.datetime(year, month, day, hours, minutes, seconds)
        dark_time = time.mktime(dark_time.timetuple()) + partial_seconds*(10**(-6))

        # dark_time is now in seconds since epoch
        dark_time_in_seconds.append(dark_time)

    sci_time_in_seconds = []
    for j in range(len(smoothed_sciences)):
        year = smoothed_sciences[j].timestamp[0:4]
        month = smoothed_sciences[j].timestamp[4:6]
        day = smoothed_sciences[j].timestamp[6:8]
        hours = smoothed_sciences[j].timestamp[8:10]
        minutes = smoothed_sciences[j].timestamp[10:12]
        seconds = smoothed_sciences[j].timestamp[12:14]
        partial_seconds = smoothed_sciences[j].timestamp[14:20]

        sci_time = dt.datetime(year, month, day, hours, minutes, seconds)
        sci_time = time.mktime(sci_time.timetuple()) + partial_seconds*(10**(-6))

        # sci_time is now in seconds since epoch
        sci_time_in_seconds.append(sci_time)

    # calculate deltas
    deltas = []
    j = 0
    for i in range(len(dark_time_in_seconds)):
        found = 0
        while found == 0 & j < len(sci_time_in_seconds):
            if abs(sci_time_in_seconds[j] - dark_time_in_seconds[i]) < abs(sci_time_in_seconds[j + 1] - dark_time_in_seconds[i]):
                found = 1
            j += 1
        deltas.append({
            "bottom": smoothed_sciences[j].smoothed_bottom_norm - dark_bottom_norms[i],
            "top": smoothed_sciences[j].smoothed_top_norm - dark_top_norms[i]
        })

    # shift smoothed norms
    result = {}
    j = 0
    for i in range(len(dark_time_in_seconds)):
        found = 0
        while found == 0 & i < len(dark_time_in_seconds):
            if abs(dark_time_in_seconds[i] - sci_time_in_seconds[j]) < abs(dark_time_in_seconds[i+1] - sci_time_in_seconds[j]):
                result[smoothed_sciences[j].sci_filename] = {
                    "bottom_norm": smoothed_sciences[j].smoothed_btm_norm - deltas[i].bottom,
                    "top_norm": smoothed_sciences[j].smoothed_top_norm - deltas[i].top
                }
                j += 1
            else:
                found = 1

        # special case for the last dark and the last few sciences
        while j <= len(sci_time_in_seconds):
            result[smoothed_sciences[j]].sci_filename = {
                "bottom_norm": smoothed_sciences[j].smoothed_btm_norm - deltas[i].bottom,
                "top_norm": smoothed_sciences[j].smoothed_top_norm - deltas[i].top
            }
            j += 1

    return result


def main(sci_filename, size, dark_filename):
    """
    This is a wrapper for calc_cma and shift_cma

    Parameters
        "sci_filename" is calc_cma's "filename" parameter
        "size" is calc_cma's "size" parameter
        "dark_filename" is shift_cma's "norm_filename" parameter

    Returns
        dict result from shift_cma
    """
    return shift_cma(dark_filename, calc_cma(sci_filename, size))