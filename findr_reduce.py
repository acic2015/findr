#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

from datetime import datetime
from work_queue import *

import argparse
import os
import socket
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
    write_message("i", "... This is a best guess IP, depending on your computing environment you may need to adjust.")
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
        except OSError:
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


def spawn_queue(port, logprefix):
    queue = WorkQueue(port)
    queue.specify_log(logprefix + "_wq.log")
    monitor_status = queue.enable_monitoring(logprefix + "_monitors")
    if not monitor_status:
        write_message("w", "Monitoring failed to initialize.")
    # queue.specify_password_file()  # TODO: Give workers a password file
    write_message("i", "Workqueue launched on port %s." % str(port))
    return queue, monitor_status


def delete_line(filepath, line):
    f = open(filepath, "r+")
    d = f.readlines()
    f.seek(0)
    for i in d:
        if i != line:
            f.write(i)
    f.truncate()
    f.close()


def check_logs(prefix):
    l = [prefix + "_all.log", prefix + "_complete.log", prefix + "_failed.log", prefix + "_usage.log"]
    f = [os.path.isfile(x) for x in l]

    if all(f):
        return [True, []]
    else:
        return [False, [l[i] for i in range(len(f)) if f[i]]]


def runFindr(configList, klipReduce, logPrefix, resume=False, retry=0):
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
    # Print welcoming message.
    if resume:
        write_message("i", "Resuming previous analysis from '%s'." % configList)
    else:
        write_message("i", "Launching a new analysis from '%s'." % configList)

    # Set number of files completed that will trigger compression, the root and starting iterator of batch names.
    compress_threshold = 100
    batch_root = "batch"
    batch_count = 0

    # Define log file names.
    alltasklog = logPrefix + "_all.log"
    completetasklog = logPrefix + "_complete.log"
    failedtasklog = logPrefix + "_failed.log"
    usagelog = logPrefix + "_usage.log"

    # Build task list.
    all_tasks = {}
    done = []
    with open(configList, 'U') as cfgin:
        for line in cfgin:
            contents = line.rstrip().split()
            all_tasks[contents[1]] = [contents[0], "read", 0]

    # If resuming, check for already complete tasks.
    if resume:
        # Generate a list of files already in directory (for clean up into tar.gz in case of crash).
        # TODO: Fix this thing!
        currents = [f for f in os.listdir('.')]
        current_batches = [f for f in currents if batch_root in f]
        if len(current_batches) > 0:
            for batch in current_batches:
                count = int(batch.split('batch')[1].split('.tar.gz')[0])
                if count >= batch_count:
                    batch_count = count + 1

        # Read & mark complete tasks.
        with open(completetasklog, 'r') as c:
            for line in c:
                details = line.strip().split('\t')
                all_tasks[details[0]][1] = "complete"

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
            exit(1)

    # Generate tasks & submit to queue, record dictionary of taskid:expected output.
    submit_count = 0
    complete_count = 0
    expect_out = {}
    with open(alltasklog, 'w', 1) as alltasks, open(completetasklog, 'w', 1) as completetasks, open(failedtasklog, 'w', 1) as failedtasks:
        for outf, details in all_tasks.iteritems():
            if details[1] != "complete":
                # Specify command.
                cfg = details[0]
                command = "%s -c %s" % (klipReduce, os.path.basename(cfg))

                # Build task.
                t = Task(command)
                t.specify_tag(command)
                t.specify_file(cfg, os.path.basename(cfg), WORK_QUEUE_INPUT, cache=False)
                t.specify_file(outf, os.path.basename(outf), WORK_QUEUE_OUTPUT, cache=False)
                # Add other file specifications as needed here.

                # If not resuming, add job to _all.log.
                if not resume:
                    alltasks.write("%s\t%s\n" % (outf, command))

                # Submit task to queue.
                taskid = q.submit(t)
                all_tasks[outf][1] = "wait"
                expect_out[taskid] = outf
                submit_count += 1
            else:
                complete_count += 1

        # Write successful launch information.
        write_message("i", "Findr launched successfully!")
        print_info(str(q.port), str(get_ip()))
        write_message("i", "%s tasks submitted to queue." % str(submit_count))
        if resume:
            write_message("i", "%s tasks already complete." % str(complete_count))

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
                expect = expect_out[t.id]
                if t.return_status != 0:
                    # Task failed. Write to failed task log.
                    all_tasks[expect][1] = "failed"
                    all_tasks[expect][2] += 1
                    # Todo: Resubmit task to queue, if under retry limit.
                    failedtasks.write("%s\t%s\t%s\n" % (expect, t.tag, str(all_tasks[expect][2])))
                else:
                    # Task succeeded. Check for valid output.
                    if os.path.exists(expect):
                        # Task succeeded & output exists. Write to complete task log.
                        completetasks.write("%s\t%s\n" % (expect, t.tag))
                        all_tasks[expect][1] = "complete"
                        if expect not in done:
                            done.append(expect)
                    else:
                        # Output is missing, alert user and write to failed tasks.
                        write_message("w", "Missing output - " + str(t.command))
                        failedtasks.write("%s\t%s\t%s\n" % (expect, t.tag, str(all_tasks[expect][2])))
                        all_tasks[expect][1] = "failed"
                        all_tasks[expect][2] += 1
                        # Todo: Resubmit task to queue, if under retry limit.

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
    # ... required argument(s).
    parser.add_argument("config", type=str, help="Configuration/outputs list (e.g. configs.list).")
    # ... optional argument(s).
    parser.add_argument("-k", "--klip", type=str, default="klipReduce", help="klipReduce path.")
    parser.add_argument("-r", "--resume", action="store_true", help="Resume an already partially complete job.")
    parser.add_argument("--retry-failed", type=int, default=0, help="Number of times to retry failed/incomplete jobs.")
    # ... parse args.
    args = parser.parse_args()

    # Confirm config file exists.
    if not os.path.isfile(args.config):
        write_message("e", "Config file does not exist.")
        exit(1)

    # Define log prefix, check for existing log files.
    log_prefix = args.config.rsplit(".", 1)[0]
    log_status = check_logs(log_prefix)
    if args.resume:
        if not log_status[0]:
            write_message("e", "Existing log file(s) could not be found: %s." % ", ".join(log_status[1]))
            exit(1)
    else:
        if len(log_status[1]) != 0:
            write_message("e", "Existing logs exist. Please move or remove: %s." % ", ".join(log_status[1]))
            exit(1)

    # Run Findr.
    runFindr(configList=args.config, klipReduce=args.klip, logPrefix=log_prefix, resume=args.resume)

    # Print concluding message.
    write_message("i", "Findr complete!")
    etime = datetime.now()
    write_message("i", "Total Run Time: %s." % str(etime - stime))
