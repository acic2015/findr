import csv
from datetime import datetime


class usageBlob(object):
    """ A collection of usage data from findr_reduce.

    """
    def __init__(self):
        self.ids = []
        self.commands = []
        self.starts = []
        self.ends = []
        self.exit_statuses = []
        self.cpu_times = []
        self.walltimes = []
        self.cores = []
        self.virtual_memory = []
        self.swap_memory = []
        self.total_processes = []
        self.max_concurrent_processes = []
        self.bytes_read = []
        self.bytes_written = []
        self.workers_connected = []
        self.workers_busy = []
        self.workers_idle = []
        self.workers_removed = []
        self.tasks_completed = []
        self.tasks_running = []
        self.tasks_waiting = []
        self.total_execute_times = []

    def __repr__(self):
        return '<usageBlob size=%s>' % (len(self.ids))

    @staticmethod
    def help():
        print("#### usageBlob Properties: Stored as index-matched lists ####")
        print("usageBlob.ids ...................... All job IDs in set")
        print("usageBlob.commands ................. Commands used by jobs")
        print("usageBlob.starts ................... Start times of jobs")
        print("usageBlob.ends ..................... End times of jobs")
        print("usageBlob.exit_statuses ............ Exit status of jobs")
        print("usageBlob.cpu_times ................ CPU time required per job")
        print("usageBlob.walltimes ................ Wall time required per job")
        print("usageBlob.cores .................... Cores used per job execution")
        print("usageBlob.virtual_memory ........... Virtual memory used per job execution")
        print("usageBlob.swap_memory .............. Swap memory used per job execution")
        print("usageBlob.total_processes .......... Total processes executed per job")
        print("usageBlob.max_concurrent_processes . Max concurrent processes per job")
        print("usageBlob.bytes_read ............... Bytes read per job")
        print("usageBlob.bytes_written ............ Bytes written per job")
        print("usageBlob.workers_connected ........ Number of workers connected at job conclusion")
        print("usageBlob.workers_busy ............. Number of workers working at job conclusion")
        print("usageBlob.workers_idle ............. Number of inactive workers at job conclusion")
        print("usageBlob.workers_removed .......... Number of workers lost or removed at job conclusion")
        print("usageBlob.tasks_completed .......... Number of tasks completed since previous check")
        print("usageBlob.tasks_running ............ Number of tasks running at job conclusion")
        print("usageBlob.tasks_waiting ............ Number of tasks waiting at job conclusion")
        print("usageBlob.total_execute_times ...... Total execution time, including transfers, per job")

        print("\n#### usageBlob Methods ####")
        print("usageBlob.apply_from_tsv(filename)\n -- Apply data from a TSV (i.e. <runprefix>_usage.log) to usageBlob")
        print("usageBlob.merge_blob(different_usageBlob)\n -- Merge data from another blob into usageBlob")
        print("usageBlob.wq_timestamp_convert(timestamp)\n -- Convert a WorkQueue timestamp to a Python timestamp")
        print("usageBlob.print_time_report\n -- Print total job count, start & end times and total time elapsed")
        print("usageBlob.help()\n -- Print this help message")

        return 1

    def apply_from_tsv(self, filename):

        def get_prop(entry, header_list, headerval):
            try:
                return entry[header_list.index(headerval)]
            except:
                return None

        with open(filename, 'r') as ipt:
            read = csv.reader(ipt, delimiter='\t')
            headers = next(read)
            for row in read:
                self.ids.append(get_prop(row, headers, "TaskID"))
                self.commands.append(get_prop(row, headers, "Command"))
                self.starts.append(get_prop(row, headers, "Start"))
                self.ends.append(get_prop(row, headers, "End"))
                self.exit_statuses.append(get_prop(row, headers, "ExitStatus"))
                self.cpu_times.append(get_prop(row, headers, "CPUTime"))
                self.walltimes.append(get_prop(row, headers, "WallTime"))
                self.cores.append(get_prop(row, headers, "Cores"))
                self.virtual_memory.append(get_prop(row, headers, "VirtualMemory"))
                self.swap_memory.append(get_prop(row, headers, "SwapMemory"))
                self.total_processes.append(get_prop(row, headers, "TotalProcesses"))
                self.max_concurrent_processes.append(get_prop(row, headers, "MaxConcurrentProcesses"))
                self.bytes_read.append(get_prop(row, headers, "BytesRead"))
                self.bytes_written.append(get_prop(row, headers, "BytesWritten"))
                self.workers_connected.append(get_prop(row, headers, "WorkersConnected"))
                self.workers_busy.append(get_prop(row, headers, "WorkersBusy"))
                self.workers_idle.append(get_prop(row, headers, "WorkersIdle"))
                self.workers_removed.append(get_prop(row, headers, "WorkersRemoved"))
                self.tasks_completed.append(get_prop(row, headers, "TasksComplete"))
                self.tasks_running.append(get_prop(row, headers, "TasksRunning"))
                self.tasks_waiting.append(get_prop(row, headers, "TasksWaiting"))
                self.total_execute_times.append(get_prop(row, headers, "TotalExecuteTime"))
        return 1

    def merge_blob(self, blob):
        self.ids += blob.ids
        self.commands += blob.commands
        self.starts += blob.starts
        self.ends += blob.ends
        self.exit_statuses += blob.exit_statuses
        self.cpu_times += blob.cpu_times
        self.walltimes += blob.walltimes
        self.cores += blob.cores
        self.virtual_memory += blob.virtual_memory
        self.swap_memory += blob.swap_memory
        self.total_processes += blob.total_processes
        self.max_concurrent_processes += blob.max_concurrent_processes
        self.bytes_read += blob.bytes_read
        self.bytes_written += blob.bytes_written
        self.workers_connected += blob.workers_connected
        self.workers_busy += blob.workers_busy
        self.workers_idle += blob.workers_idle
        self.workers_removed += blob.workers_removed
        self.tasks_completed += blob.tasks_completed
        self.tasks_running += blob.tasks_running
        self.tasks_waiting += blob.tasks_waiting
        self.total_execute_times += blob.total_execute_times

    @staticmethod
    def wq_timestamp_convert(wq_timestamp):
        # RETURNS: datetime object from timestamp
        ts = str(wq_timestamp)
        # new_ts = float(ts[0:10] + "." + ts[10:12])  # Replaced with slicing with relation to rear of list
        new_ts = float(ts[:-4][:-2] + '.' + ts[:-4][-2:])
        return datetime.fromtimestamp(new_ts)

    @staticmethod
    def wq_timestamp_seconds(wq_timeobj):
        # RETURNS: seconds from a wq timeobj (float)
        t = str(wq_timeobj)
        new_t = float(t[:-6] + '.' + t[-6:])
        return new_t

    def print_time_report(self):
        # PRINTS TO STDOUT: brief summary of times
        begin = self.wq_timestamp_convert(min(self.starts))
        finish = self.wq_timestamp_convert(max(self.ends))
        print("%s Job Records" % len(self.ids))
        print("Jobs started  : %s" % str(begin))
        print("Jobs finished : %s" % str(finish))
        print("Time elapsed  : %s" % str(finish - begin))


class runBlob(object):
    def __init__(self, prefix):
        self.prefix = prefix
        self.alltasks = []
        self.completetasks = []
        self.failedtasks = []
        self.remainingtasks = []
        self.usage = 'unlinked'

        try:
            with open(prefix + '_alltasks.log', 'U') as a:
                for l in a:
                    l = l.strip().split('\t')
                    self.alltasks.append(l[0])
            with open(prefix + '_completetasks.log') as c:
                for l in c:
                    l = l.strip()
                    self.completetasks.append(l)
            with open(prefix + '_failedtasks.log') as f:
                for l in f:
                    l = l.strip()
                    self.failedtasks.append(l)

            for t in self.alltasks:
                if t not in self.completetasks and t not in self.failedtasks:
                    self.remainingtasks.append(t)
        except:
            print("Instantiation of runBlob failed. Ensure all logs are available.")
            exit()

    def __repr__(self):
        return '<runBlob prefix=%s usage=%s>' % (self.prefix, self.usage)

    @staticmethod
    def help():
        print("#### runBlob Properties ####")
        print("runBlob.prefix ........... Prefix from which runBlob was built")
        print("runBlob.alltasks ......... All tasks submitted to run queue")
        print("runBlob.completetasks .... Tasks completed ")
        print("runBlob.failedtasks ...... Tasks attempted but failed")
        print("runBlob.remainingtasks ... Tasks not completed or failed")
        print("runBlob.usage ............ Link to a usageBlob for this run")

        print("\n#### usageBlob Methods ####")
        print("usageBlob.link_usage(usageBlob)\n -- Link this usage blob with this runBlob")
        print("usageBlob.help()\n -- Print this help message")

        return 1

    def link_usage(self, usageBlob):
        self.usage = usageBlob
