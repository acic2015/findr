from sys import argv
prefixes = argv[1:]
if len(prefixes) < 1:
    print("##---------------------- Generate Findr Report ----------------------##")
    print("| Please specify prefixes from which to generate report               |")
    print("|     i.e. './findr_report.py run1 run2 run3                          |")
    print("| If logs are in a different file, specify path along with prefixes.  |")
    print("|     i.e. './findr_report.py results/run1 results/run2 results/run3  |")
    print("##-------------------------------------------------------------------##")
    exit()
else:
    print("Generating a Findr Report. This can take a few minutes, please be patient...")

#### Finish remaining imports ####
# (waiting until after printing messages above for performance reasons)

from findr_lib import usageBlob, runBlob

import pandas as pd
from datetime import timedelta
from shutil import make_archive, copytree, rmtree
from os import path

import plotly
from plotly.graph_objs import Histogram, Pie, Scatter, Layout, Figure


#### Function Definitions ####

def mkplot(fig_obj):
    p = plotly.offline.plot(fig_obj, show_link=False, output_type='div', include_plotlyjs=False)
    return p


def convert_to_ints(list):
    l = [int(i) for i in list]
    return l


#### Build Objects ####

usage = usageBlob()
usage_by_prefix = {}
for p in prefixes:
    try:
        p_use = usageBlob()
        p_use.apply_from_tsv(p + '_usage.log')

        p_run = runBlob(p)
        p_run.link_usage(p_use)

        #usage_by_prefix[p] = p_use
        usage_by_prefix[p] = p_run
        usage.merge_blob(p_use)
    except:
        print("WARNING: Usage log for prefix %s could not be found. Skipping..." % p)
if len(usage.ids) < 1:
    print("ERROR: No results loaded. Possibly your log files could not be found, or they are empty. Exiting...")
    exit()


#### Generate Run Details ####
# TODO: Add completed vs failed tasks into report.

run_details_props = ["Start", "End", "Duration",
                     "Submitted Tasks", "Completed Tasks", "Failed Tasks", "Remaining Tasks"]
run_details_vals = []
for p in prefixes:
    start = usage.wq_timestamp_convert(min(usage_by_prefix[p].usage.starts))
    end = usage.wq_timestamp_convert(max(usage_by_prefix[p].usage.ends))
    set = [start, end, end-start,
           len(usage_by_prefix[p].alltasks),
           len(usage_by_prefix[p].completetasks),
           len(usage_by_prefix[p].failedtasks),
           len(usage_by_prefix[p].remainingtasks)]
    run_details_vals.append(set)

run_details = pd.DataFrame(run_details_vals, index=prefixes, columns=run_details_props)
run_details.sort("Start", inplace=True)

t_start = usage.wq_timestamp_convert(min(usage.starts))
t_end = usage.wq_timestamp_convert(max(usage.ends))
t_submit = run_details["Submitted Tasks"].max()
t_complete = run_details["Completed Tasks"].sum()
t_failed = run_details["Failed Tasks"].sum()
t_remain = run_details["Remaining Tasks"].min()
t_set = [[t_start, t_end, t_end-t_start,
          t_submit,
          t_complete,
          t_failed,
          t_remain]]
t_df = pd.DataFrame(t_set, index=["Total"], columns=run_details_props)
run_details = run_details.append(t_df)
run_details_table = run_details.to_html()\
    .replace('<table border="1" class="dataframe">','<table class="table table-striped">')  # Use Bootstrap CSS

#### Generate Quick Glance Overview ####

## Summary Table
qg_summary_props = ["Number of Runs Executed", "Runs Started", "Runs Ended", "Total Time Elapsed",
                    "Total Uptime", "Total Downtime", "CPU Hours Consumed"]
uptime = []
for p in prefixes:
    s = usage.wq_timestamp_convert(min(usage_by_prefix[p].usage.starts))
    e = usage.wq_timestamp_convert(max(usage_by_prefix[p].usage.ends))
    uptime.append(e - s)
uptime = reduce(lambda x, y: x+y, uptime )
downtime = (t_end-t_start) - uptime
cpu_consumption = str(timedelta(seconds=sum([usage.wq_timestamp_seconds(t) for t in usage.cpu_times])))
qg_summary_vals = [len(prefixes), t_start, t_end, t_end-t_start, uptime, downtime, cpu_consumption]
qg_df = pd.DataFrame(qg_summary_vals, index=qg_summary_props)
quickglance_table = qg_df.to_html(header=False)\
    .replace('<table border="1" class="dataframe">','<table class="table table-striped">')  # Use Bootstrap CSS

## Tasks Piechart
qg_data = {
    "data": [Pie(labels=["Complete", "Failed", "Remaining"],
                 values=[t_complete, t_failed, t_remain],
                 marker={'colors': ['green', 'red', 'grey']})],
    "layout": Layout(title="Task Status<br>(%s submitted)" % "{:,}".format(t_submit),
                     paper_bgcolor='rgba(0,0,0,0)',
                     plot_bgcolor='rgba(0,0,0,0)')
}
quickglance_plot = mkplot(qg_data)


#### Generate Plots ####

## Core Usage Histogram
core_use = {
    "data": [Histogram(x=usage.cores)],
    "layout": Layout(title="Core Usage",
                     xaxis={'title': 'Cores Used'},
                     yaxis={'title': 'Number of Jobs'})
}
core_use_plot = mkplot(core_use)

## Memory Usage Histogram
vmem_use = {
    "data": [Histogram(x=usage.virtual_memory)],
    "layout": Layout(title="Virtual Memory Usage",
                     xaxis={'title': 'Virtual Memory Used'},
                     yaxis={'title': 'Number of Jobs'})
}
vmem_use_plot = mkplot(vmem_use)

## Active Jobs Timeseries
# TODO: Separate runs into individual traces.
timeseries = {
    "data": [Scatter(x=[usage.wq_timestamp_convert(t) for t in usage.ends],
                     y=usage.tasks_running,
                     mode='markers')],
    "layout": Layout(title="Jobs running by Date/Time",
                     xaxis={'title': 'Date/Time'},
                     yaxis={'title': 'Jobs Running'})
}
timeseries_plot = mkplot(timeseries)

## CPU vs Wall Time Usage Histogram
timeuse = {
    "data": [Histogram(x=[usage.wq_timestamp_seconds(t) for t in usage.cpu_times], opacity=0.75, name="CPU Time"),
             Histogram(x=[usage.wq_timestamp_seconds(t) for t in usage.walltimes], opacity=0.57, name="Wall Time")],
    "layout": Layout(title="Distribution of Job Execution Time",
                     barmode="overlay",
                     xaxis={'title': "Time to Completion (seconds)"},
                     yaxis={'title': "Number of Jobs"})
}
timeuse_plot = mkplot(timeuse)

#### Build Summary Table ####
# TODO: Add other summary statistics (maybe a tasks-per-prefix, and others)?

sum_props = ["Cores (#)", "Virtual Memory (MB)", "Swap Memory (MB)", "CPUTime", "Walltime"]
sum_data = [convert_to_ints(usage.cores),
            convert_to_ints(usage.virtual_memory),
            convert_to_ints(usage.swap_memory),
            [usage.wq_timestamp_seconds(t) for t in usage.cpu_times],
            [usage.wq_timestamp_seconds(t) for t in usage.walltimes]]
summary = pd.DataFrame(sum_data).transpose()
summary.columns = sum_props
summary_description = summary.describe()  # Calculate summary statistics for properties in summary table
summary_description.drop('count', inplace=True)  # Removes 'count' row since it's not really needed and is ugly
summary_description["CPUTime"] = summary_description["CPUTime"].apply(lambda t: timedelta(seconds=t))
summary_description["Walltime"] = summary_description["Walltime"].apply(lambda t: timedelta(seconds=t))
summary_table = summary_description.to_html()\
    .replace('<table border="1" class="dataframe">','<table class="table table-striped">')  # Use Bootstrap CSS

#### Write final report as an HTML ####

html = '''
<html>
    <head>
        <!--<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">-->
        <!--<script type="text/javascript" src="https://cdn.plot.ly/plotly-latest.min.js"></script>-->
        <link rel="stylesheet" href="html/bootstrap-3.3.1.min.css">
        <script type="text/javascript" src="html/plotly-1.14.2.min.js"></script>
        <style>body{ margin:0 100; background:whitesmoke; }</style>
        <style>.halfplot{ height:40% ; width:47.5%; display:inline-block; }</style>
        <style>.fullplot{ height:60% ; width:95%; display:inline-block; }</style>
        <style>.center{ text-align: center; }</style>
        <style>#quickglance_table{ height:30%; width:65%; display:inline-block; }</style>
        <style>#quickglance_plot{ height:30%; width:30%; display:inline-block; }</style>

    </head>
    <body>
        <h1 class='center'>Findr Run Summary Report</h1>

        <hr>
        <h2 class='center'>Quick Glance Overview</h2>
        <div class='center' id='quickglance'>
            <div id=quickglance_table> ''' + quickglance_table + ''' </div>
            <div id=quickglance_plot> ''' + quickglance_plot + ''' </div>
        </div>


        <!--Run Details Table-->
        <hr>
        <h2 class='center'>Run Details</h2>
        ''' + run_details_table + '''

        <!-- Timing Details -->
        <hr>
        <h2 class='center'> Timing Details </h2>

        <!--Timeline Plot-->
        <h3 class='center'>Timeline</h3>
        <div class='center'>
            <div class='center fullplot'> ''' + timeseries_plot + ''' </div>
        </div>

        <!--Time Use Distribution Plot-->
        <h3 class='center'>Time for Completion Distribution</h3>
        <div class='center'>
            <div class='fullplot center'> ''' + timeuse_plot + ''' </div>
        </div>

        <!--Resource Use Details-->
        <hr>
        <h2 class='center'>Resource Use Details</h2>
        <!--Resource Use Plots-->
        <h3 class='center'>Resources Used</h3>
        <div class='center'>
            <div class='halfplot'> ''' + core_use_plot + ''' </div>
            <div class='halfplot'> ''' + vmem_use_plot + ''' </div>
        </div>

        <!--Resource Summary Table-->
        ''' + summary_table + '''
    </body>
</html>
'''

#### Write Findr Report and Create Archive ####
# TODO: Incriment findr-report directory so multiple can exist without overwrite.
# TODO: Make sure copy of HTML files works even if executed in a different directory.

if path.exists('findr-report'):
    rmtree('findr-report')
    copytree('html', 'findr-report/html')
else:
    copytree('html', 'findr-report/html')


report = open("findr-report/findr-report.html", 'w')
report.write(html)
report.close()

make_archive("findr-report", "zip", base_dir="findr-report")

print("Findr Report Ready!\n"
      "See 'findr-report/findr-report.html' or download 'findr-report.zip' to local machine for viewing")
