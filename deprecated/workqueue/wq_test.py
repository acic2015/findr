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
	port = WORK_QUEUE_DEFAULT_PORT
	#port = 0

if len(sys.argv) < 3:
	print "work_queue_example <shifts> <file1> [file2] [file3] ..."
	print "Please specify a shifts file and at least one file to center..."
	sys.exit(1)

# We create the tasks queue using the default port. If this port is already
# been used by another program, you can try setting port = 0 to use an
# available port.
try:
	q = WorkQueue(port)
	#q.specify_master_mode(WORK_QUEUE_MASTER_MODE_CATALOG)
	#q.specify_name("wq_test6")
	#q.specify_catalog_server('129.114.111.63', 9097)
except:
	print "Instantiation of Work Queue failed!"
	sys.exit(1)

print "listening on port %d for name %s..." % (q.port, q.name)

# We create and dispatch a task for each filename given in the argument list
for i in range(2, len(sys.argv)):
	fileshifts = "%s" % sys.argv[1]
	infile = "%s" % sys.argv[i]
	outfile = "cent_%s" % sys.argv[i]
	xshift = "grep '%s' ./file_shifts.txt | awk '{print $2}'" % os.path.basename(infile)
	yshift = "grep '%s' ./file_shifts.txt | awk '{print $3}'" % os.path.basename(infile)

	# Note that we write ./gzip here, to guarantee that the gzip version we
	# are using is the one being sent to the workers.
	command = "fitscent -i %s -o %s -x $(%s) -y $(%s)" % (infile, outfile, xshift, yshift)

	t = Task(command)

	# gzip is the same across all tasks, so we can cache it in the * workers.
	# Note that when specifying a file, we have to name its local * name
	# (e.g. gzip_path), and its remote name (e.g. "gzip"). Unlike the *
	# following line, more often than not these are the same. */
	t.specify_file(fileshifts, os.path.basename(fileshifts), WORK_QUEUE_INPUT, cache=True)

	# files to be compressed are different across all tasks, so we do not
	# cache them. This is, of course, application specific. Sometimes you may
	# want to cache an output file if is the input of a later task.
	t.specify_file(infile, os.path.basename(infile), WORK_QUEUE_INPUT, cache=False)
	t.specify_file(outfile, os.path.basename(outfile), WORK_QUEUE_OUTPUT, cache=False)

	# Once all files has been specified, we are ready to submit the task to the queue.
	taskid = q.submit(t)
	print "submitted task (id# %d): %s" % (taskid, t.command)

print "waiting for tasks to complete..."
while not q.empty():
	t = q.wait(5)
	if t:
		print "task (id# %d) complete: %s (return code %d)" % (t.id, t.command, t.return_status)
		if t.return_status != 0:
			# The task failed. Error handling (e.g., resubmit with new parameters, examine logs, etc.) here
			None
			#task object will be garbage collected by Python automatically when it goes out of scope

print "all tasks complete!"

#work queue object will be garbage collected by Python automatically when it goes out of scope
sys.exit(0)

