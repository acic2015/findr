#!/home/cc/library/bin/cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This program is a very simple example of how to use Work Queue.
# It accepts a list of files on the command line.
# Each file is compressed with gzip and returned to the user.

from work_queue import *

import os
import sys

# Main program
if __name__ == '__main__':
	port1 = 9000
	port2 = 9001

if len(sys.argv) < 3:
	print "work_queue_example <shifts> <file1> [file2] [file3] ..."
	print "Please specify a shifts file and at least one file to center..."
	sys.exit(1)

# We create the tasks queue using the default port. If this port is already
# been used by another program, you can try setting port = 0 to use an
# available port.
try:
	q1 = WorkQueue(port1)
	q2 = WorkQueue(port2)
except:
	print "Instantiation of Work Queue failed!"
	sys.exit(1)

print "Queue1: listening on port %d for name %s..." % (q1.port, q1.name)
print "Queue2: listening on port %d for name %s..." % (q2.port, q2.name)

# We create and dispatch a task for each filename given in the argument list
for i in range(2, len(sys.argv)):
	fileshifts = "%s" % sys.argv[1]
	infile = "%s" % sys.argv[i]
	outfile1 = "worker1_cent_%s" % sys.argv[i]
	outfile2 = "worker2_cent_%s" % sys.argv[i]
	xshift = "grep '%s' ./file_shifts.txt | awk '{print $2}'" % os.path.basename(infile)
	yshift = "grep '%s' ./file_shifts.txt | awk '{print $3}'" % os.path.basename(infile)

	# Note that we write ./gzip here, to guarantee that the gzip version we
	# are using is the one being sent to the workers.
	command1 = "fitscent -i %s -o %s -x $(%s) -y $(%s)" % (infile, outfile1, xshift, yshift)
	command2 = "fitscent -i %s -o %s -x $(%s) -y $(%s)" % (infile, outfile2, xshift, yshift)
	t1 = Task(command1)
	t2 = Task(command2)

	# gzip is the same across all tasks, so we can cache it in the * workers.
	# Note that when specifying a file, we have to name its local * name
	# (e.g. gzip_path), and its remote name (e.g. "gzip"). Unlike the *
	# following line, more often than not these are the same. */
	t1.specify_file(fileshifts, os.path.basename(fileshifts), WORK_QUEUE_INPUT, cache=True)
	t2.specify_file(fileshifts, os.path.basename(fileshifts), WORK_QUEUE_INPUT, cache=True)

	# files to be compressed are different across all tasks, so we do not
	# cache them. This is, of course, application specific. Sometimes you may
	# want to cache an output file if is the input of a later task.
	t1.specify_file(infile, os.path.basename(infile), WORK_QUEUE_INPUT, cache=False)
	t1.specify_file(outfile1, os.path.basename(outfile1), WORK_QUEUE_OUTPUT, cache=False)

	t2.specify_file(infile, os.path.basename(infile), WORK_QUEUE_INPUT, cache=False)
	t2.specify_file(outfile2, os.path.basename(outfile2), WORK_QUEUE_OUTPUT, cache=False)
	# Once all files has been specified, we are ready to submit the task to the queue.
	taskid1 = q1.submit(t1)
	taskid2 = q2.submit(t2)
	print "submitted task(1) (id# %d): %s" % (taskid1, t1.command)
	print "submitted task(2) (id# %d): %s" % (taskid2, t2.command)

print "waiting for tasks to complete..."
while not q1.empty() or not q2.empty():
	t1 = q1.wait(5)
	t2 = q2.wait(5)
	if t1:
		print "task (id# %d) complete: %s (return code %d)" % (t1.id, t1.command, t1.return_status)
		if t1.return_status != 0:
			# The task failed. Error handling (e.g., resubmit with new parameters, examine logs, etc.) here
			None
			#task object will be garbage collected by Python automatically when it goes out of scope
	if t2:
		print "task (id# %d) complete: %s (return code %d)" % (t2.id, t2.command, t2.return_status)
		if t2.return_status != 0:
			# The task failed. Error handling (e.g., resubmit with new parameters, examine logs, etc.) here
			None
			#task object will be garbage collected by Python automatically when it goes out of scope

print "all tasks complete!"

#work queue object will be garbage collected by Python automatically when it goes out of scope
sys.exit(0)
