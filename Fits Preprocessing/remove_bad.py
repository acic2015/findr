__author__ = 'Daniel Kapellusch'
import sys,copy
#dictionaries do not work well from command line, so just use: from remove_bad import clean_dic
#AOLOOPST= 'CLOSED ' is good
def clean_dic(dic):
    try:
        good_files = []
        for image in dic["SCIENCE"]:  #Search dictionary built by my other script
            if dic["SCIENCE"][image]["AOLOOPST"] == "CLOSED":
                good_files.append(image)   #store names of good files
        return(good_files)   #return those names
    except Exception as e:
        print("ERROR: "+str(e) +" is not a key in the passed dictionary")

# print(clean_dic({"SCIENCE":{"FILE":{"AOLOOPST":"CLOSED"},"FILE2":{"AOLOOPST":"OPEN"}}}))
