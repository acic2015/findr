import numpy as np

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

    with open(filename) as dat_file:
        for line in dat_file:
            img_norm_data = line.split()
            timestamps.append(img_norm_data[0][4:24])
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
        "smoothed_top_norms": top_norm_mappings,
    }