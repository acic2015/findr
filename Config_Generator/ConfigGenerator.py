from argparse import ArgumentParser
from datetime import datetime
from itertools import product
from sys import stdout
from os import mkdir, path

# # # # USE INSTRUCTIONS  # # # # # # # # # # # # # # # # # # # # # # # # # # #
#                                                                             #
# Specify a ConfigGenerator configuration file with '-i', and optionally an   #
# output filename with '-o'.                                                  #
# i.e. "python ConfigGenerator2.py -i example.cfg -o my_outputs"              #
#                                                                             #
# See example.cfg for an example and more instructions.                       #
#                                                                             #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
start = datetime.now()

# Parse arguments.
parser = ArgumentParser()
parser.add_argument('-i', type=str, required=True, help="Input configuration \
                    file (see example.cfg)")
parser.add_argument('-o', type=str, default='configs', help="Output filename \
                    (default=config)")
args = parser.parse_args()

# Read configuration file & build parameter/value items.
parameters = []
values = []
linked_parameters = {}
linked_values = {}

output_parameter = ''
output_extension = ''
with open(args.i, 'U') as cfg:
    content = cfg.readlines()
    for i in range(len(content)):
        line = content[i].strip()
        # Handle non-comment lines
        if len(line) > 0 and line[0] != '#':
            # Handle linked parameters
            if line[0] == '|':
                l_parts = map(str.strip, line.lstrip('|').split("=", 1))
                p_parts = map(str.strip, content[i-1].strip().split("=", 1))

                # iterate move up config file until unlinked field found.
                j = 1
                while p_parts[0][0] == '|':
                    j += 1
                    p_parts = map(str.strip, content[i-j].strip().split("=", 1))

                # ensure linked/parent properties have same number of elements.
                if len(eval(l_parts[1])) != len(eval(p_parts[1])):
                    print("ERROR: Linked property %s has different number of "
                          "elements than parent %s" % (l_parts[0], p_parts[0]))
                    exit()

                #
                if p_parts[0] in linked_parameters:
                    linked_parameters[p_parts[0]].append(l_parts[0])
                    linked_values[l_parts[0]] = eval(l_parts[1])
                else:
                    linked_parameters[p_parts[0]] = [l_parts[0]]
                    linked_values[l_parts[0]] = eval(l_parts[1])

            # Handle unlinked parameters
            else:
                parts = map(str.strip, line.split("=", 1))
                if parts[0] == 'OUTPUT_PARAMETER':
                    output_parameter = parts[1]
                elif parts[0] == 'OUTPUT_EXTENSION':
                    output_extension = parts[1]
                else:
                    parameters.append(parts[0])
                    values.append(eval(parts[1]))

# Calculate total number of configs to be generated & all value combinations.
total = reduce(lambda x,y: x*y, map(len, values))
perm = product(*values)

# Create output directory, if already exists warn and exit.
if not path.exists(args.o):
    mkdir(args.o)
else:
    print("ERROR: Pre-existing configs directory '%s' identified. Please "
          "rename, move, or select a new output filename before running "
          "config generator." % args.o)
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
log = open(args.o + ".list", 'w')
for n, group in enumerate(perm):
    # Extend progress bar at intervals.
    if n in marks:
        stdout.write("-")
        stdout.flush()
    # Write config file.
    f = "output_%s" % (n+1)
    fname = path.join(args.o, f + ".cfg")
    log.write("%s %s\n" % (fname, f + output_extension))
    ofile = open(fname, 'w')
    for i in range(len(parameters)):
        ofile.write("%s=%s\n" % (parameters[i], group[i]))
        if parameters[i] in linked_parameters:
            for j in range(len(linked_parameters[parameters[i]])):
                link_param = linked_parameters[parameters[i]][j]
                link_val = linked_values[link_param][values[i].index(group[i])]
                ofile.write("%s=%s\n" % (link_param, link_val))
    # Add expected output file name
    ofile.write("%s=%s\n" % (output_parameter, f + output_extension))
    ofile.close()
log.close()

# End progress bar and write total time elapsed.
stdout.write("\n")
end = datetime.now()
print("Complete! Time elapsed %s" % str(end-start))
