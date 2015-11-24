__author__ = 'Daniel Kapellusch'
# Necessary imports. Note: this is written in python 2.
import sys
import findr_lib
import astropy.io.fits as fits
import os
import multiprocessing as mp
from ConfigParser import ConfigParser
import json


def main(argv):
    if not argv:
        print "run_findr.py fits_path config_file"

    # get path and cfg file name from passed args
    fits_path = argv[0]
    config_file = argv[1]

    # read config_vals into findr_lib from file name
    print "Loading Configuration File..."
    config = ConfigParser()
    config.read(config_file)
    max_processes = config.get("findr","max_processes")
    file_shifts = config.get("findr","fileshifts")
    darkmaster = config.get("findr", "darkmaster_path")
    darksub  = config.get("findr","darksub_path")
    fitscent = config.get("findr","fitscent_path")
    outputfname = config.get("findr","outputfname")

    # Hopefully this will go away soon
    findr_lib.set_config_vals(max_processes = max_processes,file_shifts = file_shifts
                              ,darkmaster = darkmaster, darksub = darksub,
                              fitscent = fitscent)

    darklist_fn, masterdark_fn, norm_fn = "darks.list", "mastedark.fits","norm.dat"

    if not (os.path.isfile(outputfname+".json") and os.path.isfile(outputfname+".tsv")):
        # get files in dir if they are .fits
        fits_lst =  [fits_path+"/"+fit for fit in os.listdir(fits_path) if fit.endswith(".fits")]

        # get fieldnames from first fits file
        with fits.open(fits_lst[0]) as fits_file:
            items = list(set([str(header_field) for header_field in fits_file[0].header.keys()]+["FILENAME"]))

        # setup multiprocessing pool
        pool = mp.Pool(processes=int(max_processes))

        print("Extracting metadata")
        #asynchronously gather metadata
        ls = pool.map(findr_lib.get_metadata_and_sort,fits_lst)

        #generate tsv of metadata
        print("Building "+ outputfname +".tsv")
        findr_lib.make_tsv(ls,items, outputfname)

        # make dictionary from list of all metadata
        total_dic = {item["FILENAME"]:item for item in ls}

        #create json from list of metadata if config is set to allow
        print("Building "+ outputfname +".json")
        findr_lib.build_json(total_dic,outputfname)

    else: #else don't bother extracting all that stuff just read it in from the json that is present
        print("Found .json and .tsv files of specified outputfname, reading this instead...")
        with open(outputfname+".json") as json_data:
            total_dic = json.load(json_data)

    # sort metadata into dictionary of lists based on VIMTYPE
    print("Sorting header metadata")
    sorted_dic = findr_lib.sort_dic(total_dic)

    # remove science files from metadata dictionary if AOLOOPST is OPEN
    cleaned_dic = findr_lib.clean_dic(sorted_dic,total_dic)

    # Commented out while testing on my windows environment
    # # run master dark with
    # print("Running DarkMaster...")
    # findr_lib.runDarkmaster(fits_path, cleaned_dic,darklist_fn,masterdark_fn,norm_fn)
    #
    # # run subtractAndCenter
    # print("Running SubtractAndCenter...")
    # cent_dsub_files = findr_lib.subtractAndCenter(fits_path, cleaned_dic,masterdark_fn,findr_lib.file_shifts)
    #
    # #TODO Klip-reduce

    #return a dictionary of lists of filenames sorted by type
    return(cleaned_dic)


if __name__ == "__main__":
    main(sys.argv[1:])
    print("Findr Complete")