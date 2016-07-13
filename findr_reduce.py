#!/home/cc/library/bin/cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

from datetime import datetime
from work_queue import *

import os
import socket
import sys
import tarfile


def get_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def print_info(port, ip):
    wkrstr = "work_queue_worker -d all --cores 0 %s %s" % (ip, port)
    print("Listening for workers @ %s on port %s" % (ip, port))
    print("(this is a best guess IP, depending on your computing environment you may need to adjust.)")
    print("\nHINT: To start a worker, you can probably use this command: \n%s\n" % wkrstr)


def compress_remove(filelist, targzname):
    tar = tarfile.open(targzname, "w:gz")
    for f in filelist:
        tar.add(f)
    tar.close()
    for f in filelist:
        os.remove(f)


def write_worker_report(queue):
    print("Worker Report (%s): connected(%s), busy(%s), idle(%s), lost(%s)" % (str(datetime.now()),
                                                                               str(queue.stats.total_workers_connected),
                                                                               str(queue.stats.workers_busy),
                                                                               str(queue.stats.workers_idle),
                                                                               str(queue.stats.total_workers_removed)))


def write_task_report(task):
    #TaskID\tCommand\tStart\tEnd\tExitStatus\tCPUTime\tWallTime\tCores\tMaxConcurrentProcesses\tTotalProcesses\t \
    #Memory\tVirtualMemory\tSwapMemory\tBytesRead\tBytesWritten\tBytesReceived\tBytesSent\tBandwidth\tDisk\tTotalFiles
    r = task.resources_measured
    rl = [task.id, r.command, r.start, r.end, r.exit_status, r.cpu_time, r.wall_time, r.cores,
          r.max_concurrent_processes, r.total_processes, r.memory, r.virtual_memory, r.swap_memory, r.bytes_read,
          r.bytes_written, r.bytes_received, r.bytes_sent, r.bandwidth, r.disk. r.total_files]
    return "\t".join(rl) + "\n"


def runKlipReduce(klipReduce="klipReduce", logPrefix="run", configList=None, resume=False, resumeLogPrefix=None):
    """
    runKlipReduce: Distribute klipReduce tasks across resources using WorkQueue.

    ## Example Usages ##
    # New Run #
    runKlipReduce(klipReduce="klipReduce", logPrefix="try1", "configList")
    # Resume Previous Run #
    runKlipReduce(klipReduce="klipReduce", logPrefix="try2", resume=True, resumeLogPrefix="try1")

    :param klipReduce: command or path to klipReduce.
    :param logPrefix: prefix to attach to run logs.
    :param configList: list of input files & expected output files.
    :param resume: OPTIONAL, specify true if want to resume from a previous run.
    :param resumeLogPrefix: OPTIONAL, prefix of a previous run logs from which to resume.
    :return:
    """

    # Set number of files completed that will trigger compression, the root and starting iterator of batch names.
    compress_threshold = 100
    batch_root = "batch"
    batch_count = 0

    # Check to make sure specified options are valid.
    if configList == None and resumeLogPrefix == None:
        print("ERROR (runKlipReduce): Specify a configList OR resume=True and a resumeLogPrefix to run")
        exit()
    if configList != None and resumeLogPrefix != None:
        print("ERROR (runKlipReduce): configList and resumeLogPrefix specified. Use one OR the other.")
        exit()

    # Create log filename objects.
    alltasklog = logPrefix + "_alltasks.log"
    completetasklog = logPrefix + "_completetasks.log"
    failedtasklog = logPrefix + "_failedtasks.log"
    usagelog = logPrefix + "_usage.log"

    # Create the tasks queue using the default port. If this port is already
    # been used by another program, you can try setting port = 0 to use an
    # available port, or specify a port directly.
    port = WORK_QUEUE_DEFAULT_PORT
    # Launch work queue
    try:
        q = WorkQueue(port)
        q.specify_log(logPrefix + "_wq.log")
        monitoring = q.enable_monitoring()
        if not monitoring:
            print("NOTICE: Monitoring failed to initialize")
        #q.specify_password_file()  # TODO: Give these workers a password
        print("Workqueue launched on default port (%s)" % str(port))
    except:
        print("Failed to launch on default WorkQueue port. Trying to find an available port...")
        try:
            port = 0
            q = WorkQueue(port)
            q.specify_log(logPrefix + "_wq.log")
            monitoring = q.enable_monitoring()
            if not monitoring:
                print("NOTICE: Monitoring failed to initialize")
            print("Workqueue launched on available port (%s)" % str(port))
        except:
            print("Instantiation of Work Queue failed!")
            sys.exit(1)

    # Build task list, either from scratch or using resume logs.
    all_tasks = []
    done = []
    if not resume:
        with open(configList, 'U') as cfgin:
            for line in cfgin:
                contents = line.rstrip().split()
                all_tasks.append({"cfg": contents[0], "outf": contents[1]})
    elif resume:
        if resumeLogPrefix == None:
            print("ERROR (runKlipReduce): Must specify a resumeLogPrefix when resuming...")
            exit()

        # Generate a list of files already in directory (for clean up into tar.gz in case of crash).
        currents = [f for f in os.listdir('.')]
        current_batches = [f for f in currents if batch_root in f]
        if len(current_batches) > 0:
            for batch in current_batches:
                count = int(batch.split('batch')[1].split('.tar.gz')[0])
                if count >= batch_count:
                    batch_count = count+1

        resume_all = resumeLogPrefix + "_alltasks.log"
        resume_complete = resumeLogPrefix + "_completetasks.log"
        resume_failed = resumeLogPrefix + "_failedtasks.log"
        # Find incomplete tasks and store as dictionaries in remaining_tasks
        # remaining_tasks = [{"cmd": "fill_in_command", "cfg": "fill_in_cfg", "outf": "fill_in_output_file"}, ...]
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
                    all_tasks.append({"cmd": cmd, "cfg": cfg, "outf": outf})
                if contents[2] in currents:
                    done.append(contents[2])
    else:
        print("WARNING: runKlipReduce is confused and did not run. Please troubleshoot!")

    # Generate tasks & submit to queue, record dictionary of taskid:expected output.
    submit_count = 0
    expect_out = {}
    with open(alltasklog, 'w') as alltasks, open(completetasklog, 'w') as completetasks, open(failedtasklog, 'w') as failedtasks:
        for entry in all_tasks:
            # Specify command and expected output file.
            cfg = entry["cfg"]
            outf = entry["outf"]
            command = "%s -c %s" % (klipReduce, os.path.basename(cfg))
            # Add job to all tasks log.
            alltasks.write("%s\t%s\t%s\n" % (command, cfg, outf))
            # Build task.
            t = Task(command)
            t.specify_file(cfg, os.path.basename(cfg), WORK_QUEUE_INPUT, cache=False)
            t.specify_file(outf, os.path.basename(outf), WORK_QUEUE_OUTPUT, cache=False)
            # Add other file specifications as needed here (ALSO BELOW).

            # Submit task to queue.
            taskid = q.submit(t)
            expect_out[taskid] = outf
            submit_count += 1

        # Determine listening IP
        ip = get_port()

        print("WorkQueue Launched Successfully!")
        print_info(str(q.port), str(ip))
        print("...waiting for tasks to complete...")

        # Monitor queue, alert user to status, compress and remove files at specified threshold.
        if monitoring:
            use_log = open(usagelog, 'w')
            use_log.write("TaskID\tCommand\tStart\tEnd\tExitStatus\tCPUTime\tWallTime\tCores\tMaxConcurrentProcesses\t"
                          "TotalProcesses\tMemory\tVirtualMemory\tSwapMemory\tBytesRead\tBytesWritten\tBytesReceived\t"
                          "BytesSent\tBandwidth\tDisk\tTotalFiles\n")
        while not q.empty():
            t = q.wait(5)
            # Write report of worker conditions
            write_worker_report(q)
            # If tasks have returned.
            if t:
                # Print return message.
                print("%s - task (id# %d) complete: %s (return code %d)" % (str(datetime.now()), t.id, t.command, t.return_status))
                if monitoring:
                    use_log.write(write_task_report(t))
                # Check that task is actually complete.
                if t.return_status != 0:
                    # Task failed. Write to failed task log.
                    failedtasks.write("%s\n" % t.command)
                else:
                    # Task succeeded. Check for valid output.
                    expect = expect_out[t.id]
                    if os.path.exists(expect):
                        # Task succeeded & output exists. Write to complete task log.
                        completetasks.write("%s\n" % t.command)
                        done.append(expect)
                    else:
                        # Output is missing, alert user and write to failed tasks.
                        print("NOTICE: Missing output - " + str(t.command))
                        failedtasks.write("%s\n" % t.command)

                # Check if compression threshold is met, if true gzip & tar then remove uncompressed versions.
                if len(done) >= compress_threshold:
                    compress_remove(done, "%s%s.tar.gz" % (batch_root, str(batch_count)))
                    batch_count += 1
                    done = []
        # Compress any remaining outputs.
        if len(done) > 0:
            compress_remove(done, "%s%s.tar.gz" % (batch_root, str(batch_count)))

    if monitoring:
        use_log.close()
    print("MASTER: all tasks complete!")
    return 1


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
