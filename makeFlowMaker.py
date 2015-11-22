'''
Created on Oct 4, 2015

Purpose: These methods will take a dictionary of light and dark images and create a makeflow script based on those programs
Input: Dictionary containing science image keys with dark image values
Output: A file containing the makeflow instructions named "task.makeflow"

@author: Ryan Jicha
'''

# iterates through the provided dictionary and calls the createPair function
def createWorkflow(pairs):
   makeflow = open("task.makeflow", "w")

   for science, dark in pairs.iteritems():
       makeflow.write(createPair(dark,science))
   
# Creates the lines required for the makeflow file based on the provided pairs
def createPair(dark, science):
    filename = science.replace(".", "_modified.")
    requirements = dark + " " + science + " fitssub"
    command = "\tfitssub -i " + science + " -r " + dark + " > " + filename
    return filename + ": " + requirements + "\n" + command + "\n\n"

if __name__ == "__main__":
    #replace the example list with the newly generated list
    list = {'light1.fits': "dark1.fits", 'light2.fits': 'dark2.fits', 'light3.fits': 'dark3.fits'}
    createWorkflow(list)
