# Process Usage (`pu`)

## Installation

todo

## Example
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
|...|

Creat a plot (see [Python Seaborn example](./python/pu/plot.py))
![graph](./python/pu/seaborn_plot.png)
