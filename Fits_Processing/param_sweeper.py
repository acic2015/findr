__author__ = 'Dkapellusch'
import math
import os
from itertools import product
from ConfigParser import ConfigParser

'''
This script will perform the parameter sweep and generate
57500 config files for use in klip reduce.
'''

def getRefNum(imageCount):
    if imageCount +1 > 1000:
        return [250,500,750,1000,10000]
    if imageCount + 1 > 750:
        return [250,500,750,1000]
    if imageCount + 1 > 500:
        return [250,500,750]
    if imageCount + 1 > 250:
        return [250,500]
    return [250]

config  = ConfigParser()
config.read("sweeper.cfg")

minDPx = eval(config.get("sweeper","minDPx"))
minRadius = eval(config.get("sweeper","minRadius"))
maxRadius = eval(config.get("sweeper","maxRadius"))
fakePA = eval(config.get("sweeper","fakePA"))
refnum = getRefNum(int(config.get("sweeper","fileCount")))
qualityThreshold = eval(config.get("sweeper","qualityThreshold"))
quality_nums = eval(config.get("sweeper","quality_nums"))
Nmodes_fun = config.get("sweeper","Nmodes_fun")

permutation = product(fakePA,minDPx,minRadius,maxRadius,refnum,qualityThreshold)

if not os.path.exists('output'):
        os.makedirs('output')

directory = "/data/"
counter = 0.0
current_file_num = 0

print("Starting batch: 1")
for param_set in permutation:

    if current_file_num == 2500:
        print("Starting batch: "+str(int(math.ceil(counter/2500)+1)))
        current_file_num = 0
<<<<<<< HEAD
    print([param_set])
=======
    print(param_set)
>>>>>>> 641c0d5a238c1855b5f99293ec310ce3c75d150d
    counter+=1
    current_file_num+=1

    Nmodes = eval(Nmodes_fun)
    template = ("directory=" + str(directory)+"fp"+str(int(math.ceil(counter/2500))) + "\n"
                "prefix=" + "pre" + "\n"         # change if preprocessing
                "outputFile=output_"+ str(param_set[0]) +str(current_file_num)+ ".fits\n"
                "exactFName=true\n"
                "imsize=" + "265" + "\n"
                "qualityFile=/data/klipreduce/pre_file_strehl.txt" + "\n" # change if preprocessing
                "qualityThreshold="+str(param_set[5]) + "\n"
                "\n"
                "#Pre-Processing Filter" + "\n"
                "preProcess_azUSM_azW = 0.5" + "\n"
                "preProcess_azUSM_radW = 10." + "\n"
                "maskFile=/data/klipreduce/bpicb_20141103_04_mask_256.fits" + "\n"
                "preProcess_gaussUSM_fwhm = 10." + "\n"
                "skipPreProcess=true\n"       # change if preprocesssing
                "#preProcess_outputPrefix=/path/to/output/pre_\n"
                "#preProcess_only=true\n"
                "\n"
                "#KLIP Parameters" + "\n"
                "minDPx=" + str(param_set[1]) + "\n"
                "excludeMethod=pixel \n"
                "minRadius=" + str(param_set[2]) + "\n"
                "maxRadius=" + str(param_set[3]) + "\n"
                "includeRefNum="+str(param_set[4]) +"\n"
                "Nmodes="+str(Nmodes)[1:-1] +"\n"
                "#thresholdOnly=true" + "\n"
                "\n"
                "#Negative Fake Planets" + "\n"
                "fakeFileName=/data/klipreduce/bpicb_zp_20151103_04_to_be_scaled_by_strehl.fits" + "\n"
                "fakeScaleFileName=/data/klipreduce/pre_file_strehl.txt" + "\n"  # change if preprocessing
                "fakeSep=47.22,47.22" + "\n"
                "fakePA=212.31," + str(param_set[0]) + "\n"
                "fakeContrast="+ str("-5e-5,5e-5") + "\n"
                "\n"
                "#Image combination\n"
                "combineMethod=sigma\n"
                "sigmaThreshold=5"
                )

    confFileName = "output/output_"+ str(param_set[0])+str(current_file_num) + '.conf'  # change if preprocessing
    configurationFile = open(confFileName, 'w+')
    configurationFile.write(template)
    configurationFile.close()
    with open("log_file.txt",'a+') as log:
        log.writelines(("output_"+str(param_set[0]) +str(current_file_num)+ ".conf ","output_"+ str(param_set[0]) +str(current_file_num)+ ".fits\n")) # change if preprocessing
print(str(counter)+" Config files generated.")

