#!/home/cc/library/bin/cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

from datetime import datetime
from work_queue import *

import os
import socket
import sys


def runKlipReduce(klipReduce="klipReduce", logPrefix="run", configList=None, resume=False, resumeLogPrefix=None):
    # Check to make sure specified options are valid.
    if configList == None and resumeLogPrefix == None:
        print("ERROR (runKlipReduce): Specify a configList OR resume=True and a resumeLogPrefix to run")
        exit()
    if configList != None and resumeLogPrefix != None:
        print("ERROR (runKlipReduce): configList and resumeLogPrefix specified. Use one OR the other.")
        exit()

    # Create log filenames.
    alltasklog = logPrefix + "_alltasks.log"
    completetasklog = logPrefix + "_completetasks.log"
    failedtasklog = logPrefix + "_failedtasks.log"

    # Create the tasks queue using the default port. If this port is already
    # been used by another program, you can try setting port = 0 to use an
    # available port, or specify a port directly.
    port = WORK_QUEUE_DEFAULT_PORT
    # Launch work queue
    try:
        q = WorkQueue(port)
        q.specify_log(logPrefix + "_wq.log")
        #q.specify_password_file()  # TODO: Give these workers a password
    except:
        print("Instantiation of Work Queue failed!")
        sys.exit(1)

    # Submit New Run (configList specified)
    if not resume:
        # Build all tasks from configList
        all_tasks = []
        with open(configList, 'U') as cfgin:
            for line in cfgin:
                contents = line.rstrip().split()
                all_tasks.append({"cfg": contents[0], "outf": contents[1]})

        # Create, log, and dispatch a task for each config file listed in the configList
        submit_count = 0
        with open(alltasklog, 'w') as alltasks, open(completetasklog, 'w') as completetasks, open(failedtasklog, 'w') as failedtasks:
            for entry in all_tasks:
                # Specify command and expected output file.
                cfg = entry["cfg"]
                #outf = entry["outf"] + "0000.fits"  # This works if you don't have the exactFName flag in klipReduce
                outf = entry["outf"]
                command = "%s -c %s" % (klipReduce, os.path.basename(cfg))
                # Add job to all tasks log.
                alltasks.write("%s\t%s\t%s\n" % (command, cfg, outf))
                # Build task.
                t = Task(command)
                t.specify_file(cfg, os.path.basename(cfg), WORK_QUEUE_INPUT, cache=False)
                t.specify_file(outf, os.path.basename(outf), WORK_QUEUE_OUTPUT, cache=False)
                # Add other file specifications as needed here (ALSO BELOW).

                # Submit task to queue after files have been specified.
                taskid = q.submit(t)
                submit_count += 1
                #print "submitted task (id# %d): %s" % (taskid, t.command)

            # Determine listening IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8",80))
            ip = s.getsockname()[0]
            s.close()

            # Build an example worker start command
            wkrstr = "work_queue_worker -d all --cores 0 %s %s" % (str(ip), str(q.port))
            # Print useful messages
            print("WorkQueue Launched Successfully!")
            print("%s Jobs Submitted" % str(submit_count))
            print("Listening for workers @ %s on port %d" % (str(ip), q.port))
            print("(this is a best guess IP, depending on your computing environment you may need to adjust.)")
            print("\nHINT: To start a worker, you can probably use this command: \n%s\n" % wkrstr)
            print("...waiting for tasks to complete...")

            # Submit jobs & accept completion messages while queue has remaining jobs.
            while not q.empty():
                t = q.wait(5)
                if t:
                    print("%s - task (id# %d) complete: %s (return code %d)" % (str(datetime.now()), t.id, t.command, t.return_status))
                    if t.return_status != 0:
                        # Task failed. Write to failed task log.
                        failedtasks.write("%s\n" % t.command)
                    else:
                        # Task succeeded. Write to complete task log.
                        completetasks.write("%s\n" % t.command)

    # Submit a resume run.
    elif resume:
        # Create and dispatch tasks only for those tasks which were not completed (based on log files).
        if resumeLogPrefix == None:
            print("ERROR (runKlipReduce): Must specify a resumeLogPrefix when resuming...")
            exit()

        resume_all = resumeLogPrefix + "_alltasks.log"
        resume_complete = resumeLogPrefix + "_completetasks.log"
        resume_failed = resumeLogPrefix + "_failedtasks.log"
        # Find incomplete tasks and store as dictionaries in remaining_tasks
        # remaining_tasks = [{"cmd": "fill_in_command", "cfg": "fill_in_cfg", "outf": "fill_in_output_file"}, ...]
        remaining_tasks = []
        with open(resume_all, 'r') as a, open(resume_complete, 'r') as c, open(resume_failed, 'r') as f:
            comp = []
            fail = []
            for line in c:
                comp.append(line.rstrip())
            for line in f:
                fail.append(line.rstrip())
            for line in a:
                contents = line.rstrip().split('\t')
                cmd = contents[0]
                if cmd not in comp and cmd not in fail:
                    cfg = contents[1]
                    outf = contents[2]
                    remaining_tasks.append({"cmd": cmd, "cfg": cfg, "outf": outf})

        # Create, log, and dispatch a task for each remaining task.
        alltasklog = logPrefix + "_alltasks.log"
        completetasklog = logPrefix + "_completetasks.log"
        failedtasklog = logPrefix + "_failedtasks.log"
        submit_count = 0
        with open(alltasklog, 'w') as alltasks, open(completetasklog, 'w') as completetasks, open(failedtasklog, 'w') as failedtasks:
            for task in remaining_tasks:
                command = task["cmd"]
                cfg = task["cfg"]
                outf = task["outf"]
                alltasks.write("%s\t%s\n" % (command, outf))
                t = Task(command)
                t.specify_file(cfg, os.path.basename(cfg), WORK_QUEUE_INPUT, cache=False)
                t.specify_file(outf, os.path.basename(outf), WORK_QUEUE_OUTPUT, cache=False)
                # Add other file specifications as needed here (ALSO ABOVE).

                taskid = q.submit(t)
                submit_count += 1
                #print "submitted task (id# %d): %s" % (taskid, t.command)

            # Determine listening IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8",80))
            ip = s.getsockname()[0]
            s.close()

            # Build an example worker start command
            wkrstr = "work_queue_worker -d all --cores 0 %s %s" % (str(ip), str(q.port))
            # Print useful messages
            print("WorkQueue Launched Successfully!")
            print("%s Jobs Submitted" % str(submit_count))
            print("Listening for workers @ %s on port %d" % (str(ip), q.port))
            print("(this is a best guess IP, depending on your computing environment you may need to adjust.)")
            print("\nHINT: To start a worker, you can probably use this command: \n%s\n" % wkrstr)
            print("...waiting for tasks to complete...")

            while not q.empty():
                t = q.wait(5)
                if t:
                    print("%s - task (id# %d) complete: %s (return code %d)" % (str(datetime.now()), t.id, t.command, t.return_status))
                    if t.return_status != 0:
                        # Task failed. Write to failed task log.
                        failedtasks.write("%s\n" % t.command)
                    else:
                        # Task succeeded. Write to complete task log.
                        completetasks.write("%s\n" % t.command)
    else:
        print("WARNING: runKlipReduce is confused and did not run. Please troubleshoot!")

    print("MASTER: all tasks complete!")
    return 1


## Example Usages ##
# From ConfigList
#runKlipReduce(klipReduce="klipReduce", logPrefix="try1", "configList")
# Resume Previous Run
#runKlipReduce(klipReduce="klipReduce", logPrefix="try2", resume=True, resumeLogPrefix="try1")

if __name__ == "__main__":
    stime = datetime.now()
    program_args = sys.argv[1:]
    if len(program_args) < 3 or len(program_args) > 4:
        print("ERROR (arguments)")
        print("Please specify your klipReduce path "
              "(usually just 'klipReduce'), a prefix for the run, and a configList or resume options.")
        print("Syntax:")
        print("-- Launch a new run: python findr_reduce.py <klipReduce path> <run_prefix> <config-and-outputs_list>")
        print("-- Resume a previous run: python findr_reduce.py resume <klipReduce_path> <run_prefix> <resume_prefix>")
        print("Examples:")
        print("-- Launch a new run: python findr_reduce.py klipReduce try1 configList.list")
        print("-- Resume a previous run: python findr_reduce.py resume klipReduce try2 try1")
        exit()
    try:
        runKlipReduce(klipReduce=program_args[1], logPrefix=program_args[2],
                      resume=True, resumeLogPrefix=program_args[3])
    except:
        runKlipReduce(klipReduce=program_args[0], logPrefix=program_args[1], configList=program_args[2])

    print("Reductions Complete")
    ftime = datetime.now()
    print("Total Run Time: %s" % str(stime - ftime))
