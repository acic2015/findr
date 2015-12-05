
import numpy as np
import datetime as dt
import time


def calc_cma(filename, size):
    """
    This function calculates the centered moving average (CMA) of the norm values
    for the image data set's top and bottom amplifiers

    Parameters
<<<<<<< HEAD
        filename (str) -- name of dat file containing list of FITS filenames and
=======
        filename (str) -- name of dat file containing list of FITS filenames and 
>>>>>>> 491ed825bb9e990f744ea9833fb1b71132f5ad59
            the norm values for the image's top and bottom amplifiers
        size (int) -- desired subset size (between 10-20)

    Returns
        dict
    """
    timestamps = []
    bottom_norms = []
    top_norms = []

    with open(filename) as dat_file:
        for line in dat_file:
            img_norm_data = line.split()
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

    bottom_norm_mappings = {}
    top_norm_mappings = {}

    for i in range(len(timestamps)):
        bottom_norm_mappings[timestamps[i]] = bottom_norm_avgs[i]
        top_norm_mappings[timestamps[i]] = top_norm_avgs[i]

    return {
        "smoothed_btm_norms": bottom_norm_mappings,
        "smoothed_top_norms": top_norm_mappings
    }


def shift_cma(norm_filename, cma_object):

    dark_filenames = []
    timestamps = []
    bottom_norms = []
    top_norms = []

    with open(norm_filename) as dark_norm_file:
        for line in dark_norm_file:
            dark_image_data = line.split()
            dark_filenames.append(dark_image_data[0])
            timestamps.append(dark_image_data[0][4:20])
            bottom_norms.append(float(dark_image_data[1]))
            top_norms.append(float(dark_image_data[2]))

    dark_timestamps = []
    for t in timestamps:
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
        dark_timestamps.append(dark_time)

    for t in cma_object.timestamps:
        year = t[0:4]
        month = t[4:6]
        day = t[6:8]
        hours = t[8:10]
        minutes = t[10:12]
        seconds = t[12:14]
        partial_seconds = t[14:20]

        sci_time = dt.datetime(year, month, day, hours, minutes, seconds)
        sci_time = time.mktime(sci_time.timetuple()) + partial_seconds*(10**(-6))
        # sci_time is now in seconds since epoch
        sci_timestamps.append(sci_time)

    result = []

    #calculate deltas
    for i in range(dark_timestamps):
        sciences = []

        if i == 0:
            closest = 0
            for j in sci_timestamps:
                if abs(dark_timestamps[i] - sci_timestamps[j]) < abs(dark_timestamps[i+1] - sci_timestamps[j]):
                    sciences.append({
                        "filename" : cma_object[j].filename,
                        "" : cma_object.smoothed_btm_norm
                    })
        elif i == len(dark_timestamps):

        else:
            for sci in sci_timestamps:
                if:

    # shift them later
    dark_object = {
        "dark" : "",
        "sciences" : sciences
    }
    result.append(dark_object)

    return result


def main(sci_filename, dark_filename, size):
    return shift_cma(dark_filename, calc_cma(sci_filename, size))