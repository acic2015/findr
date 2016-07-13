from datetime import datetime
from itertools import product
from sys import argv, stdout
from os import mkdir, path

# # # # USE INSTRUCTIONS  # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#                                                                                                                     #
# Specify a ConfigGenerator2 config file as first command line argument (i.e. "python ConfigGenerator2.py CG2.cfg")   #
# See CG2.cfg for an example and more instructions.                                                                   #
#                                                                                                                     #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
start = datetime.now()

# Read configuration file & build parameter/value items.
parameters = []
values = []
with open(argv[1], 'U') as cfg:
    for line in cfg:
        line = line.strip()
        if len(line) > 0 and line[0][0] != '#':
            parts = map(str.strip, line.split("=", 1))
            parameters.append(parts[0])
            values.append(eval(parts[1]))

# Calculate total number of configs to be generated & all combinations of values.
total = reduce(lambda x,y: x*y, map(len, values))
perm = product(*values)

# Create output directory, if already exists warn and exit.
if not path.exists("configs"):
    mkdir("configs")
else:
    print("Pre-existing configs directory identified. Please rename or move before running config generator.")
    print("Aborting...")
    exit()

# Write starting message.
msg = "Writing %s configuration files..." % str(total)
print(msg)

# Setup progress bar.
toolbar_width = len(msg) - 2
mark_width = float(total) / toolbar_width
marks = [int(i*mark_width) for i in range(toolbar_width)]

stdout.write("[%s]" % (" " * toolbar_width))
stdout.flush()
stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['

# Write configuration files & log file.
log = open("configs.list", 'w')
for n, group in enumerate(perm):
    # Extend progress bar at intervals.
    if n in marks:
        stdout.write("-")
        stdout.flush()
    # Write config file.
    f = "output_%s" % (n+1)
    fname = path.join("configs", f + ".cfg")
    log.write(fname + ' ' + f + '.fits\n')
    ofile = open(fname, 'w')
    for i in range(len(parameters)):
        ofile.write("%s=%s\n" % (parameters[i], group[i]))
    ofile.write("exactFName=true\n")  # Add exactFName parameter
    ofile.write("outputFile=%s\n" % (f+ ".fits"))  # Add expected output file name
    ofile.close()
log.close()

# End progress bar and write total time elapsed.
stdout.write("\n")
end = datetime.now()
print("Complete! Time elapsed %s" % str(end-start))