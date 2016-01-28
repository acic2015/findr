#!/usr/bin/python2.7
__authors__ = 'AJ Stangl, Daniel Kapellusch'
import os, sys


def set_up():
    if not os.path.exists('output'):
        os.makedirs('output')


def getRefNum(imageCount):
    value = []
    value.append(250)
    if int(imageCount + 1) > 250:
        value.append(500)
    if int(imageCount + 1) > 500:
        value.append(750)
    if int(imageCount + 1) > 750:
        value.append(1000)
    if int(imageCount + 1) > 1000:
        value.append(100000)
    return value


def fake_planet():
    i = 0
    total = []
    while i < 24:
        fake = 92.0 + (i * 10.0)
        if fake != 212.0:
            fake = str(fake)
            total.append(fake)
        i = i + 1
    return total


def getNModes(imageCount):
    # round up first
    if imageCount % 2 == 1:
        imageCount += 1
    # set values always present
    value = '5,10,15'

    # do the multiples of 20
    for i in range(1, (int(imageCount) / 2) + 1):
        value += ',' + str((int(i) - 1) * 20)
    return value


def config(imageCount):
    fakePA_i = [92.0 + x * 10 for x in range(0, 24) if 92.0 + x * 10 != 212.0]
    qualityThreshold = [0.45668, 0.45225, 0.44800, 0.44444, 0.43868]
    quality_nums = {0.45668:461,0.45225:614,0.44800:769,0.44444:923,0.43868:1229}
    includeRefNum = getRefNum(imageCount)
    minDPx = [0, .5, 1., 1.5, 2.0]
    minRadius = [35, 37.5, 40, 42.5]
    maxRadius = [55, 65, 75, 85, 95]
    directory = "/data/"
    i = 0
    for counter in range(len(fakePA_i)):
        current_file_num =0
        for qt in qualityThreshold:
            for irn in includeRefNum:
                for md in minDPx:
                    for minr in minRadius:
                        for maxr in maxRadius:
                            i += 1
                            current_file_num +=1
                            fake = fakePA_i[counter]
                            Nmodes = [5, 10, 15] + [x for x in range(20, quality_nums[qt] / 2 + 1, 20)]
                            template = ("directory=" + str(directory)+"fp"+str(counter+1) + "\n"
                                            "prefix=" + "cent" + "\n"
                                            "outputFile=output_"+ str(fake) +str(current_file_num)+ ".fits\n"
                                            "exactFName=true\n"
                                            "imsize=" + "265" + "\n"
                                            "qualityFile=file_strehl.txt" + "\n"
                                            "qualityThreshold="+str(qt) + "\n"
                                            "\n"
                                            "#Pre-Processing Filter" + "\n"
                                            "preProcess_azUSM_azW = 0.5" + "\n"
                                            "preProcess_azUSM_radW = 10." + "\n"
                                            "maskFile=bpicb_20141103_04_mask_256.fits" + "\n"
                                            "preProcess_gaussUSM_fwhm = 10." + "\n"
                                            "#preProcess_outputPrefix=/path/to/output/pre_"
                                            "#preProcess_only=true\n"
                                            "\n"
                                            "#KLIP Parameters" + "\n"
                                            "minDPx=" + str(md) + "\n"
                                            "excludeMethod=pixel \n"
                                            "minRadius=" + str(minr) + "\n"
                                            "maxRadius=" + str(maxr) + "\n"
                                            "includeRefNum="+str(irn) +"\n"
                                            "Nmodes="+str(Nmodes) +"\n"
                                            "qualityFile=file_strehl.txt \n"
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
                                log.writelines(("output_"+ str(fake) +str(current_file_num)+ ".conf"," output_"+ str(fake) +str(current_file_num)+ ".fits\n"))
    print i


if __name__ == "__main__":
    set_up()
    config(2500)
