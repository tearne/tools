# Overview

Two things
* `pu`: Tool to monitor **p**rocess **u**sage (CPU & RAM) over time and save to CSV file.
* `s3util`: Primarily to report of size of S3 buckets, including object versions.

## Installation

1. [Install Rust](https://rustup.rs/)
1. Close and reopen your terminal so your path is updated.
1. Install the application you want.  For example, for `s3util`: `cargo install --git https://github.com/tearne/tools --locked --bin s3util`

This will install to `/home/[username]/.cargo/bin/`.

## `s3util` examples
Tool assumes you're using an instance profile, can't configure credentials manually at the moment.

Report the size of a single bucket to stdout:
```
s3util -v size -u my-bucket
```

Report the size of several buckets to a CSV file (defaults to `bucket_usage.csv`):
```
s3util -v size-report -u my-bucket,your-bucket,another-bucket
```

## `pu` example
```
pu -- start_my_minecraft_server.sh
```
Generates CSV file `process_usage.csv`:

|timestamp|elapsed_seconds|cpu_percent|ram_percent|ram_mb|
|-|-|-|-|-|
|2025-05-12 20:06:27|1|0.0|5|9|470.5|
|2025-05-12 20:06:28|2|180.7|8.8|700.5|
|2025-05-12 20:06:29|3|218.7|9.3|735.0|
|2025-05-12 20:06:30|4|132.1|9.9|789.5|
|...|...|...|...|...|

The [example Python code](./python/pu/plot.py) shows how to plot with Polars and Seaborn.  The simplest way to run it is to instal `uv` (fast Python package manager) and then run the script as an executable `python/pu/plot.py`.

![graph](./python/pu/seaborn_plot.png)
