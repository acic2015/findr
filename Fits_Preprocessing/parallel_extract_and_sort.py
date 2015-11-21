__author__ = 'Daniel Kapellusch'
import  astropy.io.fits as fits,os,csv,json,sys,multiprocessing as mp #necessary imports. Note: this is written in python 2.
from datetime import datetime
from os import path

def main(path):
    if not path: #set default path in the case of no passed param
        path = "sample_fits/"

    fits_lst =  [path+"/"+fit for fit in os.listdir(path) if fit.endswith(".fits")] #get files in dir if they are .fits
    with fits.open(fits_lst[0]) as fits_file:
        items = list(set([str(header_field) for header_field in fits_file[0].header.keys()]+["FILENAME"])) #get fieldnames from first fits file

    pool = mp.Pool(processes=None)  #setup multiprocessing pool
    ls = pool.map(get_metadata_and_sort,fits_lst) #asynchronously gather metadata

    make_tsv(ls,items) #generate tsv of metadata
    build_json({item["FILENAME"]:item for item in ls}) #create json from list of metadata
    return(sort_list(ls)) #return a dictionary of lists of filenames sorted by type

def get_metadata_and_sort(image):
    hdulist = fits.open(image) # open each fits file in the list
    header = hdulist[0].header #get all the metadata from the fits file hdulist
    hdulist.close()
    header["FILENAME"] = path.basename(image)
    temp = str(str(header["COMMENT"]).encode('ascii', 'ignore')) #encode in ascii as unicode doesn't play nice
    header = {key: value for key, value in header.items() #remove double comment field
            if key is not "COMMENT"}
    header["COMMENT"] = temp.replace("\n","  ") #put comments back in
    return(header)

def make_tsv(header,items):
    with open('metadata.tsv',"wb") as csvfile:    #create a file called metadata.tsv for the output
        writer = csv.DictWriter(csvfile,fieldnames=items,delimiter= "\t")  #set up the writer, header fields, and delimiter
        writer.writeheader() # write the headers to the file
        [writer.writerow({k:str(image[k]) for k in items}) for image in header]

def build_json(total_dic):
    with open("metadata.json",'w') as jsonfile: #builds json file of metadata not sorted by VIMTYPE
        json.dump(total_dic,jsonfile, separators=(',',':'),indent=4)

def sort_list(ls):
    return {"DARK":[x["FILENAME"] for x in [filter(lambda d: True if d["VIMTYPE"] =="DARK" else False,ls[index:])for index in range(len(ls))][0]],
            "SCIENCE":[x["FILENAME"] for x in [filter(lambda d: True if d["VIMTYPE"] =="SCIENCE" else False,ls[index:])for index in range(len(ls))][0]]}
    #seems like the following should be more efficent, although in my (possibly flawed) testing the above is faster
    # return {"DARK":[[ls[index]["FILENAME"]  for index in range(len(ls))if ls[index]["VIMTYPE"] == "DARK"]][0],"SCIENCE":[[ls[index]["FILENAME"]  for index in range(len(ls))if ls[index]["VIMTYPE"] == "SCIENCE"]][0]}

if __name__ =="__main__":
    start = datetime.now()
    result = main(sys.argv[1:])
    end = datetime.now()
    duration = end - start
    #This module runs in ~0.44 seconds on my machine, processing my test batch of 10 files, although if I set the processes
    # in mp.pool() to be 1 the run time becomes ~0.22 seconds
    #The serial version of this module runs in ~0.55 seconds under the same conditions 
    print "Total Execution Time (seconds): %d.%d" % (duration.seconds, duration.microseconds)
    print(result)
