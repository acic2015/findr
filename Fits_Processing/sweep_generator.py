__author__ = 'asherkhb'


# Stable
prefix = 'cent'
imSize = '256'
qualityFile = 'file_strehl.txt'
preProcess_azUSM_azW = '0.5'
preProcess_azUSM_radW = '10.'
maskFile = 'bpicb_20141103_04_mask_256.fits'
preProcess_gaussUSM_fwhm = '10.'
excludeMethod = 'pixel'

# Variable
directory = './data'
NModes = ''
outputFile = ''

# Sweep
fakePlanetDir_sweep = ['fp1', 'fp2', 'fp3', 'fp4', 'fp5', 'fp6', 'fp7', 'fp8', 'fp9', 'fp10', 'fp11', 'fp12',
                        'fp13', 'fp14', 'fp15', 'fp16', 'fp17', 'fp18', 'fp19', 'fp20', 'fp21', 'fp22', 'fp23']
qualityThreshold_sweep = [0.45668, 0.45225, 0.44800, 0.44444, 0.43868]
includeRefNum_sweep = [250,500,750,1000,100000]
minDPx_sweep = [0,.5,1.,1.5,2.0]
minRadius_sweep = [35, 37.5, 40, 42.5]
maxRadius_sweep = [55, 65, 75, 85, 95]

qualityThreshold = ''
includeRefNum = ''
minDPx = ''
minRadius = ''
maxRadius = ''

i = 0
for fakePlanetDir in fakePlanetDir_sweep:
    for qualityThreshold in qualityThreshold_sweep:
        for includeRefNum in includeRefNum_sweep:
            for minDPx in minDPx_sweep:
                for minRadius in minRadius_sweep:
                    for maxRadius in maxRadius_sweep:
                        i += 1
                        template = ("directory=/data/" + fakePlanetDir + "\n"
                                    "prefix=" + prefix + "\n"
                                    "\n"
                                    "imSize=" + imSize + "\n"
                                    "\n"
                                    "qualityFile=" + qualityFile + "\n"
                                    "qualityThreshold=" + str(qualityThreshold) + "\n"
                                    "#Pre-Processing Filters\n"
                                    "preProcess_azUSM_azW=" + preProcess_azUSM_azW + "\n"
                                    "preProcess_azUSM_radW=" + preProcess_azUSM_radW + "\n"
                                    "maskFile=" + maskFile + "\n"
                                    "preProcess_gaussUSM_fwhm=" + preProcess_gaussUSM_fwhm + "\n"
                                    "#KLIP Parameters\n"
                                    "includeRefNum=" + includeRefNum + "\n"
                                    "minDPx=" + minDPx + "\n"
                                    "excludeMethod=" + excludeMethod + "\n"
                                    "Nmodes=" + NModes + "\n"
                                    "minRadius=" + minRadius + "\n"
                                    "maxRadius=" + maxRadius + "\n"
                                    "\n"
                                    "outputFile=" + outputFile + "\n")

print i

# Parameters to sweep:

# qualityThreshold:  0.45668, 0.45225 0.44800, 0.44444, 0.43868
# includeRefNum: 250,500,750,1000,100000
# ---> This limits the size of the matrix to NxN.  Can be smart, if there are less than 750 images after thresholding, then you don't need to try the 1000 case, but should always do one larger than the number of images.
# minDPx : 0,.5,1.,1.5,2.0
# minRadius : 35, 37.5, 40, 42.5
# maxRadius : 55, 65, 75, 85, 95

# This is not a sweep, but set up the nModes vector as:
# nModes=5,10,15,20,40, (i-1)+20, . . . 0.5*number of images

# #Edit these to point to the files
# directory=../bPic_zp_sat_001/
# prefix=cent
# #Or provide this:
# #fileList=
#
# imSize=256
#
# qualityFile=file_strehl.txt
# #qualityThreshold=0.45
# qualityThreshold=0.3
#
# #Pre-Processing Filters
# preProcess_azUSM_azW = 0.5
# preProcess_azUSM_radW = 10.
# maskFile=bpicb_20141103_04_mask_256.fits
# preProcess_gaussUSM_fwhm = 10.
#
# #KLIP Parameters
# minDPx=0.5
# excludeMethod=pixel
# Nmodes=300
# minRadius=43
# maxRadius=80
#
#
# outputFile=finim