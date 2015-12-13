import sys
import json
import os
import multiprocessing as mp
from ConfigParser import ConfigParser

import astropy.io.fits as fits

import findr_lib

__author__ = "Daniel Kapellusch, Asher Baltzell"


def main(argv):
    if not argv:
        print "findr.py fits_path config_file"

    # get path and cfg file name from passed args
    fits_path = argv[0]
    config_file = argv[1]

    #  read config_vals into findr_lib from file name
    print "Loading Configuration File..."
    config = ConfigParser()
    config.read(config_file)

    max_processes = int(config.get("findr", "max_processes"))
    file_shifts = config.get("findr", "fileshifts")
    darkmaster = config.get("findr", "darkmaster_path")
    darksub = config.get("findr", "darksub_path")
    fitscent = config.get("findr", "fitscent_path")
    #klipreduce = config.get("findr", "klipreduce_path")
    outputfname = config.get("findr", "outputfname")
    smooth_window = int(config.get("findr", "smooth_window"))
    #science_norms = config.get("findr", "science_norms")
    darklist_fn = config.get("findr", "darklist_filename")
    masterdark_fn = config.get("findr", "masterdark_filename")
    norm_fn = config.get("findr", "darknorms_filename")
    imagesize = int(config.get("findr", "fullimage_size"))

    try:
        alt_darknorms = config.get("findr", "alt_darknorms")
    except Exception:
        alt_darknorms = ''

    #  Hopefully this will go away soon
    # findr_lib.set_config_vals(max_processes=max_processes, file_shifts=file_shifts,
    #                           darkmaster=darkmaster, darksub=darksub,
    #                           fitscent=fitscent, smooth_window=smooth_window, science_norms=science_norms)


    if not (os.path.isfile(outputfname + ".json") and os.path.isfile(outputfname + ".tsv")):
        #  get files in dir if they are .fits
        fits_lst = [fits_path + "/" + fit for fit in os.listdir(fits_path) if fit.endswith(".fits")]

        #  get fieldnames from first fits file
        with fits.open(fits_lst[0]) as fits_file:
            items = list(set([str(header_field) for header_field in fits_file[0].header.keys()] + ["FILENAME"]))
            items.remove("COMMENT")

        # setup multiprocessing pool
        pool = mp.Pool(processes=int(max_processes))

        print("Extracting metadata")
        # asynchronously gather metadata
        ls = pool.map(findr_lib.get_metadata_and_sort, fits_lst)

        # generate tsv of metadata
        print("Building " + outputfname + ".tsv")
        findr_lib.make_tsv(ls, items, outputfname)

        #  make dictionary from list of all metadata
        total_dic = {item["FILENAME"]: item for item in ls}

        # create json from list of metadata if config is set to allow
        print("Building " + outputfname + ".json")
        findr_lib.build_json(total_dic, outputfname)

    else:  # else don't bother extracting all that stuff just read it in from the json that is present
        print("NOTICE: Found .json and .tsv files of specified outputfname, reading this instead...")
        print("Remove these files, or change outputfname in FINDR config to recalculate these values.")
        with open(outputfname + ".json") as json_data:
            total_dic = json.load(json_data)

    # sort metadata into dictionary of lists based on VIMTYPE
    print("Sorting header metadata")
    sorted_dic = findr_lib.sort_dic(total_dic)

    #  remove science files from metadata dictionary if AOLOOPST is OPEN
    cleaned_dic = findr_lib.clean_dic(sorted_dic, total_dic)

    #  get science image norms
    # TODO: Every 100 science images run darkmaster, merge into
    scis = cleaned_dic["SCIENCE"]
    subsetsize = 1000
    subsci = [scis[i:i+subsetsize] for i in xrange(0, len(scis), subsetsize)]
    cornernorms = []
    for i, subset in enumerate(subsci):
        subset = [fits_path + '/' + image for image in subset]
        listname = 'scilist_' + str(i) + '.list'
        fitsname = 'scifits_' + str(i) + '.fits'
        normname = 'scinorm_' + str(i) + '.norms'
        cornernorms.append(normname)
        findr_lib.runDarkmaster(darkmaster, fits_path, subset, listname, fitsname, normname,
                                bot_xo=0, bot_xf=10, bot_yo=0, bot_yf=10, top_xo=0, top_xf=10, top_yo=imagesize-11, top_yf=imagesize-1,
                                medianNorm=True, medianDark=True)
    print("Consolidating norms into 'all_science_norms.norms'...")
    with open('all_science_norms.norms', 'w') as outfile:
        for fname in cornernorms:
            with open(fname) as infile:
                for line in infile:
                    outfile.write(line)
    science_norms = "all_science_norms.norms"

    #  run darkmaster
    print("Generating master dark...")
    darksl = [fits_path + '/' + image for image in cleaned_dic['DARK']]
    findr_lib.runDarkmaster(darkmaster, fits_path, darksl, darklist_fn,masterdark_fn, norm_fn,
                            medianDark=True, medianNorm=True)

    # sort norms  # TODO: Python sort, not system sort.
    print("Sorting Norms")
    sorted_scinorms = science_norms + '.sorted'
    os.system("sort -k1,1 %s > %s" % (science_norms, sorted_scinorms))

    if alt_darknorms != '':
        sorted_drknorms = alt_darknorms + '.sorted'
        os.system("sort -k1,1 %s > %s" % (alt_darknorms, sorted_drknorms))
    else:
        sorted_drknorms = norm_fn + '.sorted'
        os.system("sort -k1,1 %s > %s" % (norm_fn, sorted_drknorms))

    #  run subtractAndCenter
    print("Running SubtractAndCenter...")
    cent_dsub_files, cent_dsub_fails = findr_lib.subtractAndCenter(darksub, fitscent, darkmaster, max_processes,
                                                                   fits_path, cleaned_dic,
                                                                   masterdark_fn, sorted_drknorms, sorted_scinorms,
                                                                   smooth_window, file_shifts, imagesize)
    cent_dsub_fail_count = len(cent_dsub_fails["missing_norms"]) + len(cent_dsub_fails["missing_shifts"])
    if cent_dsub_fail_count > 0:
        print("WARNING (subtractAndCenter): %s failures" % str(cent_dsub_fail_count))
        print("-- Missing Norms: %s\n-- Missing Shifts: %s"
              % (str(len(cent_dsub_fails["missing_norms"])), str(len(cent_dsub_fails["missing_shifts"]))))

    # return a dictionary of lists of good filenames sorted by type
    return cleaned_dic


if __name__ == "__main__":
    main(sys.argv[1:])
    print("Findr Complete")
