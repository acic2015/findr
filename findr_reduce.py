#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

from datetime import datetime
from work_queue import *

import argparse
import glob
import os
import socket
import sys
import tarfile


def write_message(message_type, message):
    if message_type.lower() == "e" or message_type.lower() == "error":
        print("[Findr] %s - ERROR - %s" % (datetime.now(), message))
    elif message_type.lower() == "w" or message_type.lower() == "warning":
        print("[Findr] %s - WARNING - %s" % (datetime.now(), message))
    elif message_type.lower() == "i" or message_type.lower() == "info":
        print("[Findr] %s - INFO - %s" % (datetime.now(), message))
    else:
        print("[Findr] %s - MESSAGE - %s" % (datetime.now(), message))


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def print_info(port, ip):
    wkrstr = "work_queue_worker -d all --cores 0 %s %s" % (ip, port)
    write_message("i", "Listening for workers @ %s on port %s." % (ip, port))
    write_message("i", "... this is a best guess IP, depending on your computing environment you may need to adjust.")
    write_message("i", "... HINT: To start a worker, you can probably use this command:")
    write_message("i", "...       %s" % wkrstr)


def compress_remove(filelist, targzname):
    tar = tarfile.open(targzname, "w:gz")
    for f in filelist:
        tar.add(f)
    tar.close()
    for f in filelist:
        try:
            os.remove(f)
        except:
            write_message("w", "File not found (%s): skipping delete" % str(f))


def write_worker_report(queue):
    wkr_str = "connected(%s), busy(%s), idle(%s), lost(%s)" % (str(queue.stats.total_workers_connected),
                                                               str(queue.stats.workers_busy),
                                                               str(queue.stats.workers_idle),
                                                               str(queue.stats.total_workers_removed))
    tsk_str = "running(%s), waiting (%s)" % (str(queue.stats.tasks_running),
                                             str(queue.stats.tasks_waiting))
    write_message("i", "Status Report (time elapsed %s):" % str(datetime.now() - stime))
    write_message("i", "... Workers: %s." % wkr_str)
    write_message("i", "... Tasks: %s." % tsk_str)


def write_task_report(task, queue):
    r = task.resources_measured
    s = queue.stats
    rl = [task.id, r.command, r.start, r.end, r.exit_status,
          r.cpu_time, r.wall_time, r.cores, r.virtual_memory, r.swap_memory,
          r.total_processes, r.max_concurrent_processes, r.bytes_read, r.bytes_written,
          s.total_workers_connected, s.workers_busy, s.workers_idle, s.total_workers_removed,
          s.tasks_complete, s.tasks_running, s.tasks_waiting, s.total_execute_time]
    return "\t".join([str(l) for l in rl]) + "\n"


def spawn_queue(port, logPrefix):
    queue = WorkQueue(port)
    queue.specify_log(logPrefix + "_wq.log")
    monitor_status = queue.enable_monitoring(logPrefix + "_monitors")
    if not monitor_status:
        write_message("w", "Monitoring failed to initialize.")
    # queue.specify_password_file()  # TODO: Give workers a password file
    write_message("i", "Workqueue launched on port %s." % str(port))
    return queue, monitor_status


def runFindr(configList=None, klipReduce="klipReduce", logPrefix="run", resume=False, resumeLogPrefix=None):
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
        write_message("e", "Specify a configList OR resume=True and a resumeLogPrefix to run.")
        exit()
    if configList != None and resumeLogPrefix != None:
        write_message("e", "configList and resumeLogPrefix specified. Use one OR the other.")
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
        q, monitoring = spawn_queue(port, logPrefix)
    except:
        write_message("w", "Failed to launch on default WorkQueue port. Trying to find an available port...")
        try:
            port = 0
            q, monitoring = spawn_queue(port, logPrefix)
        except Exception as e:
            write_message("e", "Instantiation of Work Queue failed!")
            write_message("e", e)
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
            write_message("e", "Must specify a resumeLogPrefix when resuming...")
            exit()

        # Generate a list of files already in directory (for clean up into tar.gz in case of crash).
        currents = [f for f in os.listdir('.')]
        current_batches = [f for f in currents if batch_root in f]
        if len(current_batches) > 0:
            for batch in current_batches:
                count = int(batch.split('batch')[1].split('.tar.gz')[0])
                if count >= batch_count:
                    batch_count = count + 1

        resume_all = resumeLogPrefix + "_alltasks.log"
        resume_complete = resumeLogPrefix + "_completetasks.log"
        resume_failed = resumeLogPrefix + "_failedtasks.log"
        # Find incomplete tasks and store as dictionaries in remaining_tasks
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
                    all_tasks.append({"cfg": cfg, "outf": outf})
                if contents[2] in currents:
                    done.append(contents[2])
    else:
        write_message("e", "runFindr is confused and did not run. Please troubleshoot!")
        exit()

    # Generate tasks & submit to queue, record dictionary of taskid:expected output.
    submit_count = 0
    expect_out = {}
    with open(alltasklog, 'w', 1) as alltasks, open(completetasklog, 'w', 1) as completetasks, open(failedtasklog, 'w', 1) as failedtasks:
        for entry in all_tasks:
            # Specify command and expected output file.
            cfg = entry["cfg"]
            outf = entry["outf"]
            command = "%s -c %s" % (klipReduce, os.path.basename(cfg))

            # Build task.
            t = Task(command)
            t.specify_tag(command)
            t.specify_file(cfg, os.path.basename(cfg), WORK_QUEUE_INPUT, cache=False)
            t.specify_file(outf, os.path.basename(outf), WORK_QUEUE_OUTPUT, cache=False)
            # Add other file specifications as needed here (ALSO BELOW).

            # Submit task to queue.
            taskid = q.submit(t)

            # Add job to all tasks log.
            alltasks.write("%s\t%s\t%s\n" % (t.tag, cfg, outf))
            expect_out[taskid] = outf
            submit_count += 1

        # Determine listening IP
        ip = get_ip()

        write_message("i", "Findr launched successfully!")
        print_info(str(q.port), str(ip))
        write_message("i", "Waiting for %s tasks to complete..." % str(submit_count))

        # Monitor queue, alert user to status, compress and remove files at specified threshold.
        if monitoring:
            use_log = open(usagelog, 'w', 1)
            use_log.write("TaskID\tCommand\tStart\tEnd\tExitStatus\t"
                          "CPUTime\tWallTime\tCores\tVirtualMemory\tSwapMemory\t"
                          "TotalProcesses\tMaxConcurrentProcesses\tBytesRead\tBytesWritten\t"
                          "WorkersConnected\tWorkersBusy\tWorkersIdle\tWorkersRemoved\t"
                          "TasksComplete\tTasksRunning\tTasksWaiting\tTotalExecuteTime\n")

        write_worker_report(q)
        tries = 0
        while not q.empty():
            tries += 1
            t = q.wait(6)
            # Write report of worker conditions
            if not tries % 10:
                write_worker_report(q)
            # If tasks have returned.
            if t:
                # Print return message.
                write_message("i", "Task (id# %d) complete: %s (return code %d)" % (t.id, t.command, t.return_status))
                if monitoring:
                    use_log.write(write_task_report(t, q))
                # Check that task is actually complete.
                if t.return_status != 0:
                    # Task failed. Write to failed task log.
                    failedtasks.write("%s\n" % t.tag)
                else:
                    # Task succeeded. Check for valid output.
                    expect = expect_out[t.id]
                    if os.path.exists(expect):
                        # Task succeeded & output exists. Write to complete task log.
                        completetasks.write("%s\n" % t.tag)
                        if expect not in done:
                            done.append(expect)
                    else:
                        # Output is missing, alert user and write to failed tasks.
                        write_message("w", "Missing output - " + str(t.command))
                        failedtasks.write("%s\n" % t.tag)

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
    write_message("i", "All tasks complete!")
    return 1


if __name__ == "__main__":
    # Record start time.
    stime = datetime.now()

    # Parse command line arguments.
    parser = argparse.ArgumentParser()
    # ... required Argument(s)
    parser.add_argument("config", type=str, help="Configuration/outputs list (e.g. configs.list)")
    # ... optional Argument(s)
    parser.add_argument("-k", "--klip", type=str, default="klipReduce", help="klipReduce path")
    parser.add_argument("-r", "--resume", action="store_true", help="Resume an already partially complete job")
    parser.add_argument("--retry-failed", action="store_true", help="Retry failed/incomplete jobs.")

    args = parser.parse_args()

    # Sanity checks.
    # ... confirm config file exists.
    if not os.path.isfile(args.config):
        write_message("e", "Config file does not exist.")
        exit(1)
    # ... confirm log files for a "resume" job and make sure not overwriting.
    if args.resume:
        pass
    else:
        if os.path.isfile(args.config.rsplit(".", 1)[0] + "-1_alltasks.log"):
            write_message("e", "Existing run exists and would be overwritten. Please move or remove existing log files.")
            exit()
    # ... confirm retry failed is only used in conjunction with resume.
    if args.retry_failed and not args.resume:
        # TODO: Consider changing this error to simply fixing, or to ONLY retrying failed tasks.
        write_message("e", "Failed/incomplete jobs can only be retried in conjunction with a resume. Use '--resume'.")
        exit(1)

    # Print welcoming message.
    if args.resume:
        write_message("i", "Resuming previous analysis from '%s'." % args.config)
        if args.retry_failed:
            write_message("i", "Retrying failed/incomplete jobs.")
    else:
        write_message("i", "Launching a new analysis from '%s'." % args.config)

    # Define prefix or resume prefix.
    # TODO: Replace this with something better, including a much better resuming mode.
    if not args.resume:
        log_prefix = args.config.rsplit(".", 1)[0] + "-1"
        resume_log_prefix = None
    else:
        base_path = os.path.dirname(os.path.abspath(args.config))
        log_history = glob.glob(os.path.join(base_path, args.config.rsplit(".", 1)[0] + "-*_alltasks.log"))
        last = max([int(l.rsplit("-", 1)[-1].rstrip("_alltasks.log")) for l in log_history])
        log_prefix = args.config.rsplit(".", 1)[0] + "-%d" % (last + 1)
        resume_log_prefix = args.config.rsplit(".", 1)[0] + "-%d" % last
        args.config = None

    # Run Findr.
    runFindr(configList= args.config, klipReduce=args.klip, logPrefix=log_prefix,
             resume=args.resume, resumeLogPrefix=resume_log_prefix)


    # Print concluding message.
    write_message("i", "Reductions complete!")
    etime = datetime.now()
    write_message("i", "Total Run Time: %s." % str(etime - stime))
