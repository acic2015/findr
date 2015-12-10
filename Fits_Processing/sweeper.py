#!/usr/bin/python2.7
__author__ = 'AJ Stangl'
import os, sys

def set_up():
    if not os.path.exists('output'):
        os.makedirs('output')

def getRefNum(imageCount):
    value = []
    value.append(250)
    if int(imageCount) > 250:
        value.append(500)
    if int(imageCount) > 500:
        value.append(750)
    if int(imageCount) > 750:
        value.append(1000)
    if int(imageCount) > 1000:
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
    #round up first
    if imageCount % 2 == 1:
        imageCount += 1
    #set values always present
    value = '5,10,15'

    #do the multiples of 20
    for i in range(1,(int(imageCount)/2)+1):
        value += ',' + str((int(i)-1)*20)
    return value


def config(imageCount):
    qualityThreshold = [0.45668, 0.45225, 0.44800, 0.44444, 0.43868]
    includeRefNum= getRefNum(imageCount)
    minDPx= [0, .5, 1., 1.5, 2.0]
    minRadius = [35, 37.5, 40, 42.5]
    maxRadius= [55, 65, 75, 85, 95]
    directory = "tba" # FIX ME
    i = 0
    fakes = fake_planet()
    for fake in fakes:
        for qt in qualityThreshold:
            for irn in includeRefNum:
                for md in minDPx:
                    for minr in minRadius:
                        for maxr in maxRadius:
                            i +=1
                            template = ("directory=" + str(directory) + "\n"
                                        "prefix=" + "cent" + "\n"
                                        "outputFile=" + str(fake) + "\n"
                                        "imsize=" + "265" + "\n"
                                        "qualityFile=file_strehl.txt" + "\n"
                                        "qualityThreshold=0.45" + "\n"
                                        "\n"
                                        "#Pre-Processing Filter" + "\n"
                                        "preProcess_azUSM_azW = 0.5" + "\n"
                                        "preProcess_azUSM_radW = 10." + "\n"
                                        "maskFile=bpicb_20141103_04_mask_256.fits" + "\n"
                                        "preProcess_gaussUSM_fwhm = 10." + "\n"
                                        "\n"
                                        "#KLIP Parameters" + "\n"
                                        "minDPx=" + str(md) + "\n"
                                        "excludeMethod=pixel"
                                        "minRadius=" + str(minr) + "\n"
                                        "maxRadius=" + str(maxr) + "\n"
                                        "#thresholdOnly=true" + "\n"
                                        "\n"
                                        "#Negative Fake Planets" + "\n"
                                        "fakeFileName=bpicb_zp_20151103_04_to_be_scaled_by_strehl.fits" + "\n"
                                        "fakeScaleFileName=file_strehl.txt" + "\n"
                                        "fakeSep=47.22,47.22" + "\n"
                                        "fakePA=212.31," + str(fake) + "\n"
                                        "fakeContrast="+ str("-5e-5,5e-5") + "\n"
                                        "#Image combination"
                                        "combineMethod=sigma"
                                        "sigmaThreshold=5"

                                        )
                            confFileName = "output/" + fake + str(qt) + str(irn) + str(md) + str(minr) + str(maxr) + '.conf'
                            configurationFile = open(confFileName, 'w+')
                            configurationFile.write(template)
                            configurationFile.close()
    print i
if __name__ == "__main__":
    set_up()
    config(600)


