![Findr Logo](https://github.com/acic2015/findr/blob/master/img/findr_newlogo.png)


## About

Findr leverages scalable computing infrastructure for exoplanet identification.

Maintained by [Asher Baltzell](https://github.com/asherkhb)

Originally developed by the [2015 ACIC Applied Cyberinfrastructure Concepts class](https://github.com/acic2015/findr/wiki/Contributors).

## Basic Usage

```
findr_reduce [-h] [-k KLIP] [-r] [--retry-failed RETRY_FAILED] [-o OUTPUT] config

Required arguments:
  config                Configuration/outputs list (e.g. configs.list).

Optional arguments:
  -h, --help                    Show this help message and exit
  -k KLIP, --klip KLIP          klipReduce path.
  -r, --resume                  Resume an already partially complete job.
  --retry-failed RETRY_FAILED   Number of times to retry failed/incomplete jobs.
  -o OUTPUT, --output OUTPUT    Write output to file (default stdout).
```
