# Overview
## `pu`
A tool to monitor **p**rocess **u**sage (CPU & RAM) and save time series data to CSV file.

Originally created to retrospectively see if containers were being allocated too much resource, particularly in serverless setups.

## `bu`
An S3 **b**ucket **u**tility to report on size including object versions.

Created after I was surprised to learn that my bucket with 100 MiB of objects was actually 10 GiB in size, mostly due to versioning and 'object churn'.  The AWS S3 CLI [doesn't make it easy]([url](https://serverfault.com/questions/84815/how-can-i-get-the-size-of-an-amazon-s3-bucket)) to see total size including object versions.

# Installation

1. [Install Rust](https://rustup.rs/)
1. Close and reopen your terminal to update your path.
1. Install the application you want.  For example, for `bu`: `cargo install --git https://github.com/tearne/tools --locked --bin bu`

This will install to `/home/[username]/.cargo/bin/`.

# `bu` examples
Tool assumes you're using an instance profile, can't configure credentials manually at the moment.

Report the size of a single bucket/prefix to stdout:
```
bu size --url my-bucket/somePrefix
```
> s3://my-bucket/somePrefix:  
9.6 GiB (current obj. 1.1 GiB, current vers. 0 B, orphaned vers. 8.5 GiB)

Report the size of several buckets/prefixes to a CSV file (defaults to `bucket_usage.csv`):
```
bu size-report --urls my-bucket/somePrefix,your-bucket,another-bucket
```

# `pu` example
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
