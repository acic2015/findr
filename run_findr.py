__author__ = 'Daniel Kapellusch'
import sys
import findr_lib
import  astropy.io.fits as fits
import os
import multiprocessing as mp #necessary imports. Note: this is written in python 2.


def main(argv):
    if not argv:
        print "run_findr.py fits_path config_file"

    # get path and cfg file name from passed args
    fits_path = argv[0]
    config_file = argv[1]

    # read config_vals into findr_lib from file name
    print "Loading Configuration File..."
    findr_lib.get_config_vals(config_file)

    darklist_fn, masterdark_fn, norm_fn = "darks.list", "mastedark.fits","norm.dat"

    # get files in dir if they are .fits
    fits_lst =  [fits_path+"/"+fit for fit in os.listdir(fits_path) if fit.endswith(".fits")]
    # get fieldnames from first fits file
    with fits.open(fits_lst[0]) as fits_file:
        items = list(set([str(header_field) for header_field in fits_file[0].header.keys()]+["FILENAME"]))

    # setup multiprocessing pool
    pool = mp.Pool(processes=None)
    print("Extracting metadata")
    #asynchronously gather metadata
    ls = pool.map(findr_lib.get_metadata_and_sort,fits_lst)

    # sort metadata into dictionary of lists based on VIMTYPE
    print("Sorting header metadata")
    sorted_dic = findr_lib.sort_list(ls)

    #generate tsv of metadata
    print("Building metadata.tsv")
    findr_lib.make_tsv(ls,items)


    # make dictionary from list of all metadata
    total_dic = {item["FILENAME"]:item for item in ls}

    #create json from list of metadata
    print("Building metadata.json")
    findr_lib.build_json(total_dic)

    # remove science files from metadata dictionary if AOLOOPST is OPEN
    print("Cleaning sorted_dic")
    cleaned_dic = findr_lib.clean_dic(sorted_dic,total_dic)

    # run master dark with
    print("Running DarkMaster...")
    findr_lib.runDarkmaster(fits_path, cleaned_dic,darklist_fn,masterdark_fn,norm_fn)

    # run subtractAndCenter
    print("Running SubtractAndCenter...")
    cent_dsub_files = findr_lib.subtractAndCenter(cleaned_dic,masterdark_fn,findr_lib.file_shifts)

    #TODO Klip-reduce

    #return a dictionary of lists of filenames sorted by type
    return(sorted_dic)


if __name__ == "__main__":
    print(main(sys.argv[1:]))