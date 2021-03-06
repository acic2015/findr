import os
import csv
import json
from os import path, system
import multiprocessing as mp

# For CMA
import numpy as np
import datetime as dt
import csv
import time

import astropy.io.fits as fits

__author__ = "Daniel Kapellusch, Asher Baltzell, Alex Frank, Alby Chaj"

#### Class Definitions ####

class usageBlob(object):
    """ A collection of usage data from findr_reduce.

    """
    def __init__(self):
        self.ids = []
        self.commands = []
        self.starts = []
        self.ends = []
        self.exit_statuses = []
        self.cpu_times = []
        self.walltimes = []
        self.cores = []
        self.virtual_memory = []
        self.swap_memory = []
        self.total_processes = []
        self.max_concurrent_processes = []
        self.bytes_read = []
        self.bytes_written = []
        self.workers_connected = []
        self.workers_busy = []
        self.workers_idle = []
        self.workers_removed = []
        self.tasks_completed = []
        self.tasks_running = []
        self.tasks_waiting = []
        self.total_execute_times = []

    def __repr__(self):
        return '<usageBlob size=%s>' % (len(self.ids))

    @staticmethod
    def help():
        print("#### usageBlob Properties: Stored as index-matched lists ####")
        print("usageBlob.ids ...................... All job IDs in set")
        print("usageBlob.commands ................. Commands used by jobs")
        print("usageBlob.starts ................... Start times of jobs")
        print("usageBlob.ends ..................... End times of jobs")
        print("usageBlob.exit_statuses ............ Exit status of jobs")
        print("usageBlob.cpu_times ................ CPU time required per job")
        print("usageBlob.walltimes ................ Wall time required per job")
        print("usageBlob.cores .................... Cores used per job execution")
        print("usageBlob.virtual_memory ........... Virtual memory used per job execution")
        print("usageBlob.swap_memory .............. Swap memory used per job execution")
        print("usageBlob.total_processes .......... Total processes executed per job")
        print("usageBlob.max_concurrent_processes . Max concurrent processes per job")
        print("usageBlob.bytes_read ............... Bytes read per job")
        print("usageBlob.bytes_written ............ Bytes written per job")
        print("usageBlob.workers_connected ........ Number of workers connected at job conclusion")
        print("usageBlob.workers_busy ............. Number of workers working at job conclusion")
        print("usageBlob.workers_idle ............. Number of inactive workers at job conclusion")
        print("usageBlob.workers_removed .......... Number of workers lost or removed at job conclusion")
        print("usageBlob.tasks_completed .......... Number of tasks completed since previous check")
        print("usageBlob.tasks_running ............ Number of tasks running at job conclusion")
        print("usageBlob.tasks_waiting ............ Number of tasks waiting at job conclusion")
        print("usageBlob.total_execute_times ...... Total execution time, including transfers, per job")

        print("\n#### usageBlob Methods ####")
        print("usageBlob.apply_from_tsv(filename)\n -- Apply data from a TSV (i.e. <runprefix>_usage.log) to usageBlob")
        print("usageBlob.merge_blob(different_usageBlob)\n -- Merge data from another blob into usageBlob")
        print("usageBlob.wq_timestamp_convert(timestamp)\n -- Convert a WorkQueue timestamp to a Python timestamp")
        print("usageBlob.print_time_report\n -- Print total job count, start & end times and total time elapsed")
        print("usageBlob.help()\n -- Print this help message")

        return 1

    def apply_from_tsv(self, filename):

        def get_prop(entry, header_list, headerval):
            try:
                return entry[header_list.index(headerval)]
            except:
                return None

        with open(filename, 'r') as ipt:
            read = csv.reader(ipt, delimiter='\t')
            headers = next(read)
            for row in read:
                self.ids.append(get_prop(row, headers, "TaskID"))
                self.commands.append(get_prop(row, headers, "Command"))
                self.starts.append(get_prop(row, headers, "Start"))
                self.ends.append(get_prop(row, headers, "End"))
                self.exit_statuses.append(get_prop(row, headers, "ExitStatus"))
                self.cpu_times.append(get_prop(row, headers, "CPUTime"))
                self.walltimes.append(get_prop(row, headers, "WallTime"))
                self.cores.append(get_prop(row, headers, "Cores"))
                self.virtual_memory.append(get_prop(row, headers, "VirtualMemory"))
                self.swap_memory.append(get_prop(row, headers, "SwapMemory"))
                self.total_processes.append(get_prop(row, headers, "TotalProcesses"))
                self.max_concurrent_processes.append(get_prop(row, headers, "MaxConcurrentProcesses"))
                self.bytes_read.append(get_prop(row, headers, "BytesRead"))
                self.bytes_written.append(get_prop(row, headers, "BytesWritten"))
                self.workers_connected.append(get_prop(row, headers, "WorkersConnected"))
                self.workers_busy.append(get_prop(row, headers, "WorkersBusy"))
                self.workers_idle.append(get_prop(row, headers, "WorkersIdle"))
                self.workers_removed.append(get_prop(row, headers, "WorkersRemoved"))
                self.tasks_completed.append(get_prop(row, headers, "TasksComplete"))
                self.tasks_running.append(get_prop(row, headers, "TasksRunning"))
                self.tasks_waiting.append(get_prop(row, headers, "TasksWaiting"))
                self.total_execute_times.append(get_prop(row, headers, "TotalExecuteTime"))
        return 1

    def merge_blob(self, blob):
        self.ids += blob.ids
        self.commands += blob.commands
        self.starts += blob.starts
        self.ends += blob.ends
        self.exit_statuses += blob.exit_statuses
        self.cpu_times += blob.cpu_times
        self.walltimes += blob.walltimes
        self.cores += blob.cores
        self.virtual_memory += blob.virtual_memory
        self.swap_memory += blob.swap_memory
        self.total_processes += blob.total_processes
        self.max_concurrent_processes += blob.max_concurrent_processes
        self.bytes_read += blob.bytes_read
        self.bytes_written += blob.bytes_written
        self.workers_connected += blob.workers_connected
        self.workers_busy += blob.workers_busy
        self.workers_idle += blob.workers_idle
        self.workers_removed += blob.workers_removed
        self.tasks_completed += blob.tasks_completed
        self.tasks_running += blob.tasks_running
        self.tasks_waiting += blob.tasks_waiting
        self.total_execute_times += blob.total_execute_times

    @staticmethod
    def wq_timestamp_convert(wq_timestamp):
        # RETURNS: datetime object from timestamp
        ts = str(wq_timestamp)
        # new_ts = float(ts[0:10] + "." + ts[10:12])  # Replaced with slicing with relation to rear of list
        new_ts = float(ts[:-4][:-2] + '.' + ts[:-4][-2:])
        return dt.datetime.fromtimestamp(new_ts)

    @staticmethod
    def wq_timestamp_seconds(wq_timeobj):
        # RETURNS: seconds from a wq timeobj (float)
        t = str(wq_timeobj)
        new_t = float(t[:-6] + '.' + t[-6:])
        return new_t

    def print_time_report(self):
        # PRINTS TO STDOUT: brief summary of times
        begin = self.wq_timestamp_convert(min(self.starts))
        finish = self.wq_timestamp_convert(max(self.ends))
        print("%s Job Records" % len(self.ids))
        print("Jobs started  : %s" % str(begin))
        print("Jobs finished : %s" % str(finish))
        print("Time elapsed  : %s" % str(finish - begin))


class runBlob(object):
    def __init__(self, prefix):
        self.prefix = prefix
        self.alltasks = []
        self.completetasks = []
        self.failedtasks = []
        self.remainingtasks = []
        self.usage = 'unlinked'

        try:
            with open(prefix + '_alltasks.log', 'U') as a:
                for l in a:
                    l = l.strip().split('\t')
                    self.alltasks.append(l[0])
            with open(prefix + '_completetasks.log') as c:
                for l in c:
                    l = l.strip()
                    self.completetasks.append(l)
            with open(prefix + '_failedtasks.log') as f:
                for l in f:
                    l = l.strip()
                    self.failedtasks.append(l)

            for t in self.alltasks:
                if t not in self.completetasks and t not in self.failedtasks:
                    self.remainingtasks.append(t)
        except:
            print("Instantiation of runBlob failed. Ensure all logs are available.")
            exit()

    def __repr__(self):
        return '<runBlob prefix=%s usage=%s>' % (self.prefix, self.usage)

    @staticmethod
    def help():
        print("#### runBlob Properties ####")
        print("runBlob.prefix ........... Prefix from which runBlob was built")
        print("runBlob.alltasks ......... All tasks submitted to run queue")
        print("runBlob.completetasks .... Tasks completed ")
        print("runBlob.failedtasks ...... Tasks attempted but failed")
        print("runBlob.remainingtasks ... Tasks not completed or failed")
        print("runBlob.usage ............ Link to a usageBlob for this run")

        print("\n#### usageBlob Methods ####")
        print("usageBlob.link_usage(usageBlob)\n -- Link this usage blob with this runBlob")
        print("usageBlob.help()\n -- Print this help message")

        return 1

    def link_usage(self, usageBlob):
        self.usage = usageBlob


#### Function Definitions ####


def get_metadata_and_sort(image):
    with fits.open(image) as hdulist:
        header = hdulist[0].header  # get all the metadata from the fits file hdulist
    header["FILENAME"] = path.basename(image)
    return {key: value for key, value in header.items()  # remove comment field
            if key is not "COMMENT"}


def make_tsv(header, items, outputfname):
    with open(outputfname + ".tsv", "wb") as csvfile:  # create a file called metadata.tsv for the output
        # set up the writer, header fields, and delimiter
        writer = csv.DictWriter(csvfile, fieldnames=items, delimiter="\t")
        writer.writeheader()  # write the headers to the file
        [writer.writerow({k: str(image[k]) for k in items}) for image in header]


def build_json(total_dic, outputfname):
    with open(outputfname + ".json", 'w') as jsonfile:  # builds json file of metadata not sorted by VIMTYPE
        json.dump(total_dic, jsonfile, separators=(',', ':'), indent=4)


def sort_dic(total_dic):
    # sort total_dic into dictionary by VIMTYPE
    sorted_dic = {"SCIENCE": [], "DARK": []}
    [sorted_dic["SCIENCE"].append(total_dic[i]["FILENAME"]) if total_dic[i]["VIMTYPE"] == "SCIENCE"
     else sorted_dic["DARK"].append(total_dic[i]["FILENAME"]) for i in total_dic]
    return sorted_dic


def clean_dic(sorted_dic, total_dic):
    cleaned_dic = {'SCIENCE': [], "DARK": sorted_dic["DARK"]}
    for image in sorted_dic["SCIENCE"]:  # Search dictionary built by my other script
        if total_dic[image]["AOLOOPST"] == "CLOSED":
            cleaned_dic["SCIENCE"].append(image)  # store names of good files
    return cleaned_dic  # return those names


def writeListCfg(lst, cfgname):
    """
    Write out a config file from a list.
    - Entries: 'listItem\n'
    :param lst: List to be written as a config file.
    :param cfgname: Filename or path/to/filename for config file.
    :return: Config filename or path/to/filename
    """
    cfg_out = open(cfgname, 'w')
    for e in lst:
        cfg_out.write(str(e) + '\n')
    cfg_out.close()
    return cfgname


def writeDictCfg(dct, cfgname):
    """
    Write out a config file from a dictionary.
    - Entries: 'key=value\n'
    :param dct: Dictionary to be written as a config file.
    :param cfgname: Filename or path/to/filename for config file.
    :return: Config filename or path/to/filename
    """
    cfg_out = open(cfgname, 'w')
    for k, v in dct.iteritems():
        cfg_out.write('%s=%s\n' % (str(k), str(v)))
    cfg_out.close()
    return cfgname


def runDarkmaster(darkmaster, image_path, imgs, darklist_filename, masterdark_filename, norm_filename,
                  bot_xo=None, bot_xf=None, bot_yo=None, bot_yf=None,
                  top_xo=None, top_xf=None, top_yo=None, top_yf=None,
                  width=None, height=None,
                  config=None, medianNorm=True, medianDark=True):
    print("...Running DarkMaster")

    #global darkmaster

    # Write dark images to config file.
    writeListCfg(imgs, darklist_filename)
    # Fill out required parameters
    options = '--fileListFile=%s --darkFileName=%s --normFileName=%s' % (darklist_filename,
                                                                         masterdark_filename,
                                                                         norm_filename)
    # Fill out bottom/top normalization coordinates, if present.
    if bot_xo != None and bot_xf != None and bot_yo != None and bot_yf != None \
            and top_xo != None and top_xf != None and top_yo != None and top_yf != None:
        options += ' --bot_xo=%s --bot_xf=%s --bot_yo=%s --bot_yf=%s' % (str(bot_xo), str(bot_xf),
                                                                         str(bot_yo), str(bot_yf))
        options += ' --top_xo=%s --top_xf=%s --top_yo=%s --top_yf=%s' % (str(top_xo), str(top_xf),
                                                                         str(top_yo), str(top_yf))
    # Fill out height/width of centered normalization region (overrides normalization coordinates), if present.
    if width and height:
        options += ' --width=%s --height=%s' % (str(width), str(height))
    # Add median options, if present
    if medianNorm:
        options += ' --medianNorm'
    if medianDark:
        options += ' --medianDark'
    # Build & call darkmaster command.
    cmd = darkmaster + ' ' + options
    print cmd
    system(cmd)
    return 1


def prependToFilename(filename, prepending):
    """
    Prepend Text to Filename.
    :param filename: Filename or path/to/filename to be modified.
    :param prepending: String to prepend to filename.
    :return: Modified filename or path/to/filename.
    """
    b = os.path.basename(filename)
    n = prepending + b
    return filename.replace(b, n)


def spawnDsubCmd(darksub, science_img, dark_img, norm_bot=None, norm_top=None):
    """
    Spawn a darksub command.
    :param science_img: Science image filename or path/to/filename.
    :param dark_img: Master dark filename or path/to/filename.
    :param norm_bot: Multiplicative scaling to apply to the bottom amplifier (optional).
    :param norm_top: Multiplicative scaling to apply to the top amplifier (optional).
    :return: darksub_command, subtracted_fiilename
    """
    dsub_out = './dsub/' + os.path.basename(prependToFilename(science_img, 'dsub_'))
    dsub_opts = '--inputFile=%s --darkFile=%s --outputFile=%s' % (science_img, dark_img, dsub_out)
    if norm_bot:
        dsub_opts += ' --norm_bot=%s' % str(norm_bot)
    if norm_top:
        dsub_opts += ' --norm_top=%s' % str(norm_top)
    dsub_cmd = darksub + ' ' + dsub_opts
    return dsub_cmd, dsub_out


def spawnCentCmd(fitscent, subtracted_img, xshift, yshift):  # TODO: Make imSize specifiable
    """
    Spawn a fitscent command.
    :param subtracted_img: Dark subtracted science image.
    :param xshift: X shift to apply to image.
    :param yshift: Y shift to apply to image.
    :return: fitscent_command, centered_filename
    """
    cent_out = './cent/' + os.path.basename(prependToFilename(subtracted_img, 'cent_'))
    cent_opts = '--input=%s --imSize=256 --x=%s --y=%s --output=%s' % (subtracted_img, str(xshift), str(yshift), cent_out)
    cent_cmd = fitscent + ' ' + cent_opts
    return cent_cmd, cent_out


def loadShifts(shifts_file):
    shifts = {}
    with open(shifts_file, 'r') as s:
        for l in s:
            if len(l) <= 1 or l[0] == '#':
                pass
            else:
                c = l.split()
                shifts[c[0]] = {'x': c[1], 'y': c[2]}
    return shifts


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
            dark_timestamps.append(dark_image_data[0][4:24])
            dark_bottom_norms.append(float(dark_image_data[1]))
            dark_top_norms.append(float(dark_image_data[2]))

    # convert all timestamps to seconds since epoch
    dark_time_in_seconds = []
    for t in dark_timestamps:
        year = int(t[0:4])
        month = int(t[4:6])
        day = int(t[6:8])
        hours = int(t[8:10])
        minutes = int(t[10:12])
        seconds = int(t[12:14])
        partial_seconds = float(t[14:20])

        dark_time = dt.datetime(year, month, day, hours, minutes, seconds)
        dark_time = time.mktime(dark_time.timetuple()) + partial_seconds*(10**(-6))

        # dark_time is now in seconds since epoch
        dark_time_in_seconds.append(dark_time)

    sci_time_in_seconds = []
    for j in range(len(smoothed_sciences)):
        t = smoothed_sciences[j]["timestamp"]
        year = int(t[0:4])
        month = int(t[4:6])
        day = int(t[6:8])
        hours = int(t[8:10])
        minutes = int(t[10:12])
        seconds = int(t[12:14])
        partial_seconds = float(t[14:20])

        sci_time = dt.datetime(year, month, day, hours, minutes, seconds)
        sci_time = time.mktime(sci_time.timetuple()) + partial_seconds*(10**(-6))

        # sci_time is now in seconds since epoch
        sci_time_in_seconds.append(sci_time)

    # calculate deltas
    deltas = []
    j = 0
    for i in range(len(dark_time_in_seconds)):  # AKB Added to escape IndexError
        found = 0
        while found == 0 and j < (len(sci_time_in_seconds) - 1):
            if abs(sci_time_in_seconds[j] - dark_time_in_seconds[i]) < abs(sci_time_in_seconds[j + 1] - dark_time_in_seconds[i]):
                found = 1
            j += 1
        deltas.append({
            "bottom": smoothed_sciences[j]["smoothed_btm_norm"] - dark_bottom_norms[i],
            "top": smoothed_sciences[j]["smoothed_top_norm"] - dark_top_norms[i]
        })

    # shift smoothed norms
    result = {}
    j = 0
    for i in range(len(dark_time_in_seconds)):  # AKB Added to escape IndexError
        found = 0
        while found == 0 and i < (len(dark_time_in_seconds) - 1) and j < len(sci_time_in_seconds):
            if abs(dark_time_in_seconds[i] - sci_time_in_seconds[j]) < abs(dark_time_in_seconds[i+1] - sci_time_in_seconds[j]):
                result[smoothed_sciences[j]["sci_filename"]] = {  
                    "bottom_norm": smoothed_sciences[j]["smoothed_btm_norm"] - deltas[i]["bottom"],
                    "top_norm": smoothed_sciences[j]["smoothed_top_norm"] - deltas[i]["top"]
                }
                j += 1
            else:
                found = 1

    # special case for the last dark and the last few sciences
    while j < len(sci_time_in_seconds):
        result[smoothed_sciences[j]["sci_filename"]] = {
            "bottom_norm": smoothed_sciences[j]["smoothed_btm_norm"] - deltas[len(deltas) - 1]["bottom"],
            "top_norm": smoothed_sciences[j]["smoothed_top_norm"] - deltas[len(deltas) - 1]["top"]
        }
        j += 1

    return result


def runProcess(call):
    print call
    os.system(call)
    return 1


def subtractAndCenter(darksub, fitscent, darkmaster, max_processes, image_path, image_dict,
                      masterdark, darknorms, scinorms, smoothwindow, shifts_file, imsize):
    print("Subtracting and Centering")
    # Build list of science images to process.
    sciences = [image_path + '/' + image for image in image_dict['SCIENCE']]
    # Load shift values from file to memory.
    fileshifts = loadShifts(shifts_file)
    # Build norms and store in memory.
    fnorms = shift_cma(darknorms, calc_cma(scinorms, smoothwindow))

    # Define necessary variables.
    scmds = []
    souts = []
    ccmds = []
    couts = []

    fail_count = 0
    fail_files = {"missing_norms": [], "missing_shifts": []}
    # Build up commands for each science image.
    for img in sciences:
        img_name = os.path.basename(img)

        # Get norm values.
        try:
            tnorm = fnorms[img_name]["top_norm"]
            bnorm = fnorms[img_name]["bottom_norm"]
        except:
            #print "Warning (subtractAndCenter): %s not found in norms" % str(img_name)
            fail_files["missing_norms"].append(img_name)
            fail_count += 1
            continue

        # Get shift values
        try:
            xshift = fileshifts[img_name]['x']
            yshift = fileshifts[img_name]['y']
        except:
            #print "Warning (subtractAndCenter): %s not found in shifts_file" % str(img_name)
            fail_files["missing_shifts"].append(img_name)
            fail_count += 1
            continue  # Skip remaining task

        # Build subtraction task.
        ds_cmd, ds_out = spawnDsubCmd(darksub, img, masterdark, norm_bot=bnorm, norm_top=tnorm)
        scmds.append(ds_cmd)
        souts.append(ds_out)

        # Build centering task.
        cn_cmd, cn_out = spawnCentCmd(fitscent, ds_out, xshift=xshift, yshift=yshift)
        ccmds.append(cn_cmd)
        couts.append(cn_out)

    # Execute subtraction tasks (parallel).
    sub_pool = mp.Pool(processes=max_processes)
    sub_pool.map(runProcess, scmds)

    # Validate Results
    # Run darkmaster on subset of dark-subtracted frames in 10x10 corners.
    # When plotting the norms, should be ~0 with noise, and top/bottom should be close..
    thinby = 100
    print("...generating confirmation files (thinned by every %s image)" % str(thinby))
    #conf_list = [image_path + '/' + os.path.basename(souts[i]) for i in xrange(0, len(souts), thinby)]
    conf_list = [souts[i] for i in xrange(0, len(souts), thinby)]
    runDarkmaster(darkmaster, image_path, conf_list, "confirmation.list", "confirmation.fits", "confirmation.norms",
                  bot_xo=0, bot_xf=10, bot_yo=0, bot_yf=10, top_xo=0, top_xf=10, top_yo=imsize-11, top_yf=imsize-1,
                  medianNorm=True, medianDark=True)

    # Execute centering tasks (parallel).
    cent_pool = mp.Pool(processes=max_processes)
    cent_pool.map(runProcess, ccmds)

    # Return list of final filenames and failed files.
    return couts, fail_files


def getSciNorms(darkmaster, sciences_list, img_path, subset_size, imagesize, normfilename):
    print("...calculating norm values from science image 10x10px corners in %s image batches" % str(subset_size))
    subsci = [sciences_list[i:i+subset_size] for i in xrange(0, len(sciences_list), subset_size)]
    cornernorms = []
    othertmp = []
    for i, subset in enumerate(subsci):
        subset = [img_path + '/' + image for image in subset]
        listname = 'scilist_' + str(i) + '.list'
        fitsname = 'scifits_' + str(i) + '.fits'
        normname = 'scinorm_' + str(i) + '.norms'
        cornernorms.append(normname)
        othertmp.append(listname)
        othertmp.append(fitsname)
        runDarkmaster(darkmaster, img_path, subset, listname, fitsname, normname,
                      bot_xo=0, bot_xf=10, bot_yo=0, bot_yf=10, top_xo=0, top_xf=10, top_yo=imagesize-11, top_yf=imagesize-1,
                      medianNorm=True, medianDark=True)
    print("...consolidating science norms into '%s'" % normfilename)
    with open(normfilename, 'w') as outfile:
        for fname in cornernorms:
            with open(fname) as infile:
                for line in infile:
                    outfile.write(line)

    print("...removing temporary norm files")
    for f in cornernorms:
        os.remove(f)
    for f in othertmp:
        os.remove(f)

    return normfilename

def normSort(normfile):
    sorted_norms = normfile + '.sorted'
    print("...sorting norm file (%s -> %s)" % (normfile, sorted_norms))
    os.system("sort -k1,1 %s > %s" % (normfile, sorted_norms))
    return sorted_norms