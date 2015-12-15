__author__ = 'Dkapellusch'
import math
import os,sys
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

if len(sys.argv)<2:
    print("Please specify paths to sweeper.cfg and template.cfg")
    exit()
config  = ConfigParser()
config.read(sys.argv[1])

#Define your paramsets for permutations here
#Define your paramset either here in the form of a list, or as a list in sweeper.cfg
minDPx = eval(config.get("sweeper","minDPx"))
minRadius = eval(config.get("sweeper","minRadius"))
maxRadius = eval(config.get("sweeper","maxRadius"))
fakePA = eval(config.get("sweeper","fakePA"))
refnum = getRefNum(int(config.get("sweeper","fileCount")))
qualityThreshold = eval(config.get("sweeper","qualityThreshold"))
quality_nums = eval(config.get("sweeper","quality_nums"))

with open(sys.argv[2],'r') as cfg_file:
    template = cfg_file.readlines()

#put the paramset at the end of this permutation line and in your template.cfg put a reference to it in the form of
#myParam = str(param_set[x])
permutation = product(fakePA,minDPx,minRadius,maxRadius,refnum,qualityThreshold)
ls = list(permutation)

if not os.path.exists('configs'):
        os.makedirs('configs')

directory = "/data/"
counter = 0.0
current_file_num = 0
divisor = len(ls)/len(fakePA)

print("Starting batch: 1")
for param_set in ls:
    if current_file_num == divisor:
        print("Starting batch: "+str(int(math.ceil(counter/divisor)+1)))
        current_file_num = 0
    counter+=1
    current_file_num+=1
    for line in template:
        if line.startswith("#"):
            pass
        else:
            if "directory" in line:
                key,val = line.split("=")
                line = key+"="+str(eval(val))+"\n"
            if "param_set"in line:
                key,val = line.split("=")
                line = key+"="+str(eval(val))+"\n"
                if key =="Nmodes":
                    line = key+"="+str(eval(val))[1:-1]+"\n"
        confFileName = "configs/output_"+ str(param_set[0])+str(current_file_num) + '.conf'  # change if preprocessing
        with open(confFileName,'a') as conf:
            conf.write(line)
    with open("configs.list",'a+') as log:
        log.writelines(("configs/output_"+str(param_set[0]) +str(current_file_num)+ ".conf ","output_"+ str(param_set[0]) +str(current_file_num)+ ".fits\n")) # change if preprocessing
print(str(counter)+" Config files generated.")


if "__name__" =="__main__":
    if "-h" in sys.argv:
        print("Run this to generate all of the configs for the param_sets you have defined\n"\
              "To define your paramsets put them in sweeper.cfg in the form of a list\n"\
               "You must then put that paramset at the end of the permutations line on line 42\n"\
               " Then you must put that parameter in the template.cfg in the form of myparam = str(param_set[x])\n"\
              "Where x is the 0-based position of that paramset in the permutations parameters")