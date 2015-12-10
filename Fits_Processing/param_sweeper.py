__author__ = 'Dkapellusch'
import math
import os
from itertools import product
'''
This script will perform the parameter sweep and generate
57500 config files for use in klip reduce.
'''

def getRefNum(imageCount):
    if imageCount +1 > 1000:
        return [250,500,750,1000,1000]
    if imageCount + 1 > 750:
        return [250,500,750,1000]
    if imageCount + 1 > 500:
        return [250,500,750]
    if imageCount + 1 > 250:
        return [250,500]
    return [250]

minDPx = [0, .5, 1., 1.5, 2.0]
minRadius = [35, 37.5, 40, 42.5]
maxRadius = [55, 65, 75, 85, 95]
fakePA = [92.0 + x * 10 for x in range(0, 24) if 92.0 + x * 10 != 212.0]
refnum = getRefNum(1001)
qualityThreshold = [0.45668, 0.45225, 0.44800, 0.44444, 0.43868]
quality_nums = {0.45668:461,0.45225:614,0.44800:769,0.44444:923,0.43868:1229}

permutations = product(minDPx,minRadius,maxRadius,fakePA,refnum,qualityThreshold)

if not os.path.exists('output'):
        os.makedirs('output')

directory = "/data/"
counter = 0
current_file_num = 0

print("Starting batch: 1")

for param_set in permutations:

    if current_file_num == 2500:
        print("Starting batch: "+str(int(math.ceil(counter/2500)+1)))
        current_file_num = 0

    counter+=1
    current_file_num+=1

    Nmodes = [5, 10, 15] + [x for x in range(20, quality_nums[param_set[5]] / 2 + 1, 20)]
    fake = fakePA[int(math.ceil(counter/2500))-1]

    template = ("directory=" + str(directory)+"fp"+str(int(math.ceil(counter/2500))) + "\n"
                "prefix=" + "cent" + "\n"
                "outputFile=output_"+ str(fake) +str(current_file_num)+ ".fits\n"
                "exactFName=True\n"
                "imsize=" + "265" + "\n"
                "qualityFile=file_strehl.txt" + "\n"
                "qualityThreshold="+str(param_set[5]) + "\n"
                "\n"
                "#Pre-Processing Filter" + "\n"
                "preProcess_azUSM_azW = 0.5" + "\n"
                "preProcess_azUSM_radW = 10." + "\n"
                 "maskFile=bpicb_20141103_04_mask_256.fits" + "\n"
                "preProcess_gaussUSM_fwhm = 10." + "\n"
                "#preProcess_outputPrefix=/path/to/output/pre_\n"
                "#preProcess_only=true\n"
                "\n"
                "#KLIP Parameters" + "\n"
                "minDPx=" + str(param_set[0]) + "\n"
                "excludeMethod=pixel \n"
                "minRadius=" + str(param_set[1]) + "\n"
                "maxRadius=" + str(param_set[2]) + "\n"
                "includeRefNum="+str(param_set[4]) +"\n"
                "Nmodes="+str(Nmodes)[1:-1] +"\n"
                "#thresholdOnly=true" + "\n"
                "\n"
                "#Negative Fake Planets" + "\n"
                "fakeFileName=bpicb_zp_20151103_04_to_be_scaled_by_strehl.fits" + "\n"
                "fakeScaleFileName=file_strehl.txt" + "\n"
                "fakeSep=47.22,47.22" + "\n"
                "fakePA=212.31," + str(fake) + "\n"
                "fakeContrast="+ str("-5e-5,5e-5") + "\n"
                "\n"
                "#Image combination\n"
                "combineMethod=sigma\n"
                "sigmaThreshold=5"
                )

    confFileName = "output/output_"+ str(fake) +"_"+str(current_file_num) + '.conf'
    configurationFile = open(confFileName, 'w+')
    configurationFile.write(template)
    configurationFile.close()
    with open("log_file.txt",'a+') as log:
        log.writelines(("output_"+ str(fake) +str(current_file_num)+ ".fits "," output_"+ str(fake) +str(current_file_num)+ ".conf\n"))

print(str(counter)+" Config files generated.")
