__author__ = 'Daniel Kapellusch'
import  astropy.io.fits as fits,os,csv,json,sys #necessary imports. Note: this is written in python 2.

def main(path):
    if not path:
        path = "sample_fits/"
    return(build_json(create_metadata_and_sort(path))) #call all functions below, passing each function's return to the next

def create_metadata_and_sort(path):
    lst = os.listdir(path) #set path for list of fits files, change this to the correct path on your machine
    total_dic = {}
    with fits.open((path+lst[0])) as fits_file:
        items = list(set([str(x) for x in fits_file[0].header.keys()]+["FILENAME"])) #get fieldnames from first fits file

    with open('metadata.tsv',"wb") as csvfile:    #create a file called metadata.tsv for the output
        writer = csv.DictWriter(csvfile,fieldnames=items,delimiter= "\t")  #set up the writer, header fields, and delimiter
        writer.writeheader() # write the headers to the file

        for i in lst: # iterate through the list of fits files
            hdulist = fits.open(path+i) # open each fits file in the list
            header = hdulist[0].header #get all the metadata from the fits file hdulist
            hdulist.close()
            header["FILENAME"] = i
            temp = str(str(header["COMMENT"]).encode('ascii', 'ignore')) #encode in ascii as unicode doesn't play nice
            header = {key: value for key, value in header.items() #remove double comment field
                 if key is not "COMMENT"}
            header["COMMENT"] = temp.replace("\n","  ") #put comments back in
            total_dic[header["FILENAME"]] = header #add fits image metadata to dictionary
            writer.writerow({k:str(header[k]) for k in items})  #write metadata to tsv
    return(total_dic)

def build_json(total_dic):
    sorted_dic = {"SCIENCE":{},"DARK":{}}
    for image in total_dic: #sort dictionary by darks and science
        if total_dic[image]["VIMTYPE"] == "SCIENCE":
            sorted_dic["SCIENCE"][image] = total_dic[image]
        elif total_dic[image]["VIMTYPE"] == "DARK":
            sorted_dic["DARK"][image] = total_dic[image]
        else:
            print("was nothing somehow")

    with open("metadata.json",'w') as jsonfile: #builds json file
        json.dump(sorted_dic,jsonfile, separators=(',',':'),indent=4)
    print("DONE!")
    return(sorted_dic)

if __name__ =="__main__":
    main(sys.argv[1:])