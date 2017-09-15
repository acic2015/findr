#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

from datetime import datetime
from work_queue import *

import argparse
import os
import socket
import tarfile


def write_message(message_type, message, destination=None):
    """Write informative message.

    Writes a Findr-styled informative message either to stdout, or to a destination output file. Four types of messages
    are currently supported: "INFO" (informative messages), "WARNING" (non-critical errors), "ERROR" (critical errors),
    and "MESSAGE" (unspecified message type). ERROR messages should always be accompanied by an exit() call.

    Args:
        message_type (str): Type of message - "i"/"info", "w"/"warning", "e"/"error".
        message (str): Text to accompany message.
        destination (file -or- None, optional): Open file to write messages. Default = None (write to stdout).

    Returns:
        int: Always returns 1.

    """
    # Set message.
    if message_type.lower() == "e" or message_type.lower() == "error":
        s = "[Findr] %s - ERROR - %s" % (datetime.now(), message)
    elif message_type.lower() == "w" or message_type.lower() == "warning":
        s = "[Findr] %s - WARNING - %s" % (datetime.now(), message)
    elif message_type.lower() == "i" or message_type.lower() == "info":
        s = "[Findr] %s - INFO - %s" % (datetime.now(), message)
    else:
        s = "[Findr] %s - MESSAGE - %s" % (datetime.now(), message)

    # Write to destination.
    if destination is None:
        print(s)
    else:
        destination.write(s + "\n")

    return 1


def get_ip():
    """Get IP address.

    Opens a socket to 8.8.8.8 to obtains a best-guess IP address for the machine. Sometimes returns a local IP, so care
    should be taken.

    Returns:
        str: Best-guess IP address for the machine

    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def print_info(port, ip):
    """Print Queue Information

    Prints a helpful summary of queue information to stdout.

    Args:
        port (str -or- int): Port on which queue is listening.
        ip (str): IP address where queue is listening.

    Returns:
        int: Always returns 1.

    """
    wkrstr = "work_queue_worker -d all --cores 0 %s %s" % (ip, str(port))
    write_message("i", "Listening for workers @ %s on port %s." % (ip, str(port)))
    write_message("i", "... This is a best guess IP, depending on your computing environment you may need to adjust.")
    write_message("i", "... HINT: To start a worker, you can probably use this command:")
    write_message("i", "...       %s" % wkrstr)

    return 1


def compress_remove(filelist, targzname, logfile):
    """Compress file list, remove uncompressed versions.

    Compresses all files named in a list (filelist) to a tarball (targzname). Writes any warnings using write_message().

    Args:
        filelist (list): List of filenames/paths to compress
        targzname (str): Name of tarball to output. Should always end in ".tar.gz".
        logfile (str -or- None): Open log file for write_message(), or None for stdout.

    Returns:
        str: Output tarball filename.

    """
    # Compress files.
    tar = tarfile.open(targzname, "w:gz")
    for f in filelist:
        tar.add(f)
    tar.close()
    # Remove uncompressed versions.
    for f in filelist:
        try:
            os.remove(f)
        except OSError:
            write_message("w", "File not found (%s): skipping delete" % str(f), logfile)
    return targzname


def write_worker_report(queue, logfile):
    """Write queue status report.

    Writes a summary of the current queue, including worker stats (connected, busy, idle, and lost) and jobs (running,
    waiting). Writes using write_message().

    Args:
        queue (work_queue::WorkQueue): Active queue of interest.
        logfile (str -or- None): Open log file for write_message(), or None for stdout.

    Return:
        int: Always returns 1.

    """
    wkr_str = "connected(%s), busy(%s), idle(%s), lost(%s)" % (str(queue.stats.total_workers_connected),
                                                               str(queue.stats.workers_busy),
                                                               str(queue.stats.workers_idle),
                                                               str(queue.stats.total_workers_removed))
    tsk_str = "running(%s), waiting (%s)" % (str(queue.stats.tasks_running),
                                             str(queue.stats.tasks_waiting))
    write_message("i", "Status Report (time elapsed %s):" % str(datetime.now() - stime), logfile)
    write_message("i", "... Workers: %s." % wkr_str, logfile)
    write_message("i", "... Tasks: %s." % tsk_str, logfile)
    return 1


def write_task_report(task, queue):
    """Generate task usage report entry.

    Generates a report of task computational usage, including task ID, executed command, start time, end time, exit
    status, CPU time, wall time, cores used, virtual memory used, swap memory used, total processes executed, max
    concurrent processes, bytes read, bytes written, number of workeres connected at completion, workers busy at
    completion, workers idle at completion, workers lost at completion, number of tasks complete, number of tasks
    running, number of tasks waiting, and the total Findr execution time.

    Args:
        task (work_queue::Task): Completed task, which has had resource monitoring enabled.
        queue (work_queue::WorkQueue): Active queue.

    Returns:
        str: Tab-separated usage values.

    """
    r = task.resources_measured
    s = queue.stats
    rl = [task.id, r.command, r.start, r.end, r.exit_status,
          r.cpu_time, r.wall_time, r.cores, r.virtual_memory, r.swap_memory,
          r.total_processes, r.max_concurrent_processes, r.bytes_read, r.bytes_written,
          s.total_workers_connected, s.workers_busy, s.workers_idle, s.total_workers_removed,
          s.tasks_complete, s.tasks_running, s.tasks_waiting, s.total_execute_time]
    return "\t".join([str(l) for l in rl]) + "\n"


def spawn_queue(port, logprefix, logfile):
    """ Spawn a job queue.

    Launch a job queue listening on a given port. Writes warnings using write_message().

    Args:
        port (int -or- str): Port which queue should listen for workers.
        logprefix (str): Prefix for queue log.
        logfile (str -or- None): Open log file for write_message(), or None for stdout.

    Returns:
        work_queue::WorkQueue: Queue object.
        bool: Status of enabling compute monitoring. True for success, False for failure.

    """
    queue = WorkQueue(port)
    queue.specify_log(logprefix + "_wq.log")
    monitor_status = queue.enable_monitoring(logprefix + "_monitors")
    if not monitor_status:
        write_message("w", "Monitoring failed to initialize.", logfile)
    # queue.specify_password_file()  # TODO: Give workers a password file
    write_message("i", "Workqueue launched on port %s." % str(port), logfile)
    return queue, monitor_status


def create_task(cmd, cfgf, outpf):
    """ Create a task.

    Create a klipReduce task, to be submitted to the queue.

    Args:
        cmd (str): Command-line text for task execution (e.g. 'klipReduce -c my_config.cfg')
        cfgf (str): Path to configuration file.
        outpf (str): Expected output.

    Returns:
        work_queue::Task: Task object.

    """
    # Build task.
    t = Task(cmd)
    t.specify_tag(cmd)
    t.specify_file(cfgf, os.path.basename(cfgf), WORK_QUEUE_INPUT, cache=False)
    t.specify_file(outpf, os.path.basename(outpf), WORK_QUEUE_OUTPUT, cache=False)
    # Add other file specifications as needed here.
    return t


def check_logs(prefix):
    """Check logs.

    Checks for existing Findr logs. Currently checks for <prefix>_all.log", <prefix>_complete.log",
    <prefix>_failed.log", <prefix>_usage.log".

    Args:
        prefix (str): Logfile prefix.

    Returns:
        list: List of found and missing log files [[found, logs], [missing, logs]].

    """
    l = [prefix + "_all.log", prefix + "_complete.log", prefix + "_failed.log", prefix + "_usage.log"]
    f = [os.path.isfile(x) for x in l]

    # Return [[found], [missing]]
    return [[l[i] for i in range(len(f)) if f[i]], [l[i] for i in range(len(f)) if not f[i]]]


def runFindr(configList, klipReduce, logPrefix, resume=False, retry=0, logfile=None):
    """ Run Findr.

    Handles major operations of Findr.

    Args:
        configList (str): Path to config file (see Examples/configs.list).
        klipReduce (str): klipReduce path, if klipReduce is in path this can just be 'klipReduce' .
        logPrefix (str): Prefix for log files. This is usually the base of configList (e.g. configs.list -> configs)
        resume (bool, optional): Resume previous run, requires existing log files with given prefix. Default = False.
        retry (int, optional): [NOT IMPLEMENTED] Number of times to retry failed tasks. Default = 0.
        logfile (file -or- None, optional): Open file object to write messages, or None for stdout. Default = None.

    Return:
        int: Always returns 1.

    """
    # Print welcoming message.
    if resume:
        write_message("i", "Resuming previous analysis from '%s'." % configList, logfile)
    else:
        write_message("i", "Launching a new analysis from '%s'." % configList, logfile)

    # Set number of files completed that will trigger compression, the root and starting iterator of batch names.
    compress_threshold = 100
    batch_root = "batch"
    batch_count = 0

    # Define log file names.
    alltlog = logPrefix + "_all.log"
    completetlog = logPrefix + "_complete.log"
    failedtlog = logPrefix + "_failed.log"
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
        with open(completetlog, 'r') as c:
            for line in c:
                details = line.strip().split('\t')
                all_tasks[details[0]][1] = "complete"

    # Create the tasks queue using the default port. If this port is already
    # been used by another program, you can try setting port = 0 to use an
    # available port, or specify a port directly.
    port = WORK_QUEUE_DEFAULT_PORT
    # Launch work queue
    try:
        q, monitoring = spawn_queue(port, logPrefix, logfile)
    except:
        write_message("w", "Failed to launch on default WorkQueue port. Trying to find an available port...", logfile)
        try:
            port = 0
            q, monitoring = spawn_queue(port, logPrefix, logfile)
        except Exception as e:
            write_message("e", "Instantiation of Work Queue failed!", logfile)
            write_message("e", e, logfile)
            exit(1)

    # Generate tasks & submit to queue, record dictionary of taskid:expected output.
    submit_count = 0
    complete_count = 0
    task_details = {}
    with open(alltlog, 'a+', 1) as allt, open(completetlog, 'a+', 1) as completet, open(failedtlog, 'a+', 1) as failedt:
        for outf, details in all_tasks.iteritems():
            if details[1] != "complete":
                # Specify command.
                cfg = details[0]
                command = "%s -c %s" % (klipReduce, os.path.basename(cfg))

                # Build task.
                t = create_task(command, cfg, outf)

                # If not resuming, add job to _all.log.
                if not resume:
                    allt.write("%s\t%s\n" % (outf, command))

                # Submit task to queue.
                taskid = q.submit(t)
                all_tasks[outf][1] = "wait"
                task_details[taskid] = [outf, cfg, command]
                submit_count += 1
            else:
                complete_count += 1

        # Write successful launch information.
        write_message("i", "Findr launched successfully!", logfile)
        print_info(str(q.port), str(get_ip()))
        write_message("i", "%s tasks submitted to queue." % str(submit_count), logfile)
        if resume:
            write_message("i", "%s tasks already complete." % str(complete_count), logfile)

        # Monitor queue, alert user to status, compress and remove files at specified threshold.
        if monitoring and not resume:
            use_log = open(usagelog, 'a+', 1)
            use_log.write("TaskID\tCommand\tStart\tEnd\tExitStatus\t"
                          "CPUTime\tWallTime\tCores\tVirtualMemory\tSwapMemory\t"
                          "TotalProcesses\tMaxConcurrentProcesses\tBytesRead\tBytesWritten\t"
                          "WorkersConnected\tWorkersBusy\tWorkersIdle\tWorkersRemoved\t"
                          "TasksComplete\tTasksRunning\tTasksWaiting\tTotalExecuteTime\n")

        write_worker_report(q, logfile)
        tries = 0
        while not q.empty():
            tries += 1
            t = q.wait(6)
            # Write report of worker conditions
            if not tries % 10:
                write_worker_report(q, logfile)
            # If tasks have returned.
            if t:
                # Print return message.
                write_message("i", "Task (id# %d) complete: %s (return code %d)" % (t.id, t.command, t.return_status),
                              logfile)
                if monitoring:
                    use_log.write(write_task_report(t, q))

                # Check that task is actually complete.
                expect = task_details[t.id][0]
                if t.return_status != 0:
                    # Task failed. Write to failed task log.
                    write_message("w", "... failure (return code %s)." % str(t.return_status), logfile)
                    all_tasks[expect][1] = "failed"
                    all_tasks[expect][2] += 1
                    if all_tasks[expect][2] <= retry:
                        # Todo: Resubmit task to queue, if under retry limit.
                        failedt.write("%s\t%s\t%s\n" % (expect, t.tag, str(all_tasks[expect][2])))
                    else:
                        failedt.write("%s\t%s\t%s\n" % (expect, t.tag, str(all_tasks[expect][2])))
                else:
                    # Task succeeded. Check for valid output.
                    if os.path.exists(expect):
                        # Task succeeded & output exists. Write to complete task log.
                        write_message("i", "... success.", logfile)
                        completet.write("%s\t%s\n" % (expect, t.tag))
                        all_tasks[expect][1] = "complete"
                        if expect not in done:
                            done.append(expect)
                        else:
                            write_message("w", "Task %s complete, but '%s' already existed." % (str(t.id), expect),
                                          logfile)
                    else:
                        # Output is missing, alert user and write to failed tasks.
                        write_message("w", "... failure. (missing output %s)." % str(t.expect), logfile)
                        all_tasks[expect][1] = "failed"
                        all_tasks[expect][2] += 1
                        if all_tasks[expect][2] <= retry:
                            # Todo: Resubmit task to queue, if under retry limit.
                            failedt.write("%s\t%s\t%s\n" % (expect, t.tag, str(all_tasks[expect][2])))
                        else:
                            failedt.write("%s\t%s\t%s\n" % (expect, t.tag, str(all_tasks[expect][2])))

                # Check if compression threshold is met, if true gzip & tar then remove uncompressed versions.
                if len(done) >= compress_threshold:
                    compress_remove(done, "%s%s.tar.gz" % (batch_root, str(batch_count)), logfile)
                    batch_count += 1
                    done = []
        # Compress any remaining outputs.
        if len(done) > 0:
            compress_remove(done, "%s%s.tar.gz" % (batch_root, str(batch_count)), logfile)

    if monitoring:
        use_log.close()
    write_message("i", "All tasks complete!", logfile)
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
    parser.add_argument("-o", "--output", type=str, default=None, help="Write output to file (default stdout).")
    # ... parse args.
    args = parser.parse_args()

    # Print first message.
    write_message("i", "Findr starting.")

    # Confirm config file exists.
    if not os.path.isfile(args.config):
        write_message("e", "Config file does not exist.")
        exit(1)

    # Define log prefix, check for existing log files.
    log_prefix = args.config.rsplit(".", 1)[0]
    log_status = check_logs(log_prefix)
    if args.resume:
        if len(log_status[1]) > 0:
            write_message("e", "Existing log file(s) could not be found: %s." % ", ".join(log_status[1]))
            exit(1)
    else:
        if len(log_status[0]) > 0:
            write_message("e", "Existing logs exist. Please move or remove: %s." % ", ".join(log_status[0]))
            exit(1)

    # Open output file if specified.
    if args.output is not None:
        log_out = open(args.output, "a+", 1)
        write_message("i", "Writing messages to %s" % args.output)
    else:
        log_out = None

    # Run Findr.
    runFindr(configList=args.config, klipReduce=args.klip, logPrefix=log_prefix,
             resume=args.resume, retry=args.retry_failed, logfile=log_out)

    # Print concluding message.
    write_message("i", "Findr complete!", log_out)
    etime = datetime.now()
    write_message("i", "Total Run Time: %s." % str(etime - stime), log_out)

    # Close output file if specified
    if args.output is not None:
        log_out.close()

    # Print final message.
    write_message("i", "Done.")
