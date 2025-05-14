#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "polars",
#   "seaborn",
#   "matplotlib"
# ]
# ///


import os
from pathlib import Path
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

if 'VIRTUAL_ENV' not in os.environ:
    exit("Run this script from a venv to avoid polluting your system.")

data_dir = Path('process_usage.csv')

data = pl.read_csv(data_dir)
print(data)

melted = data.unpivot(on=["cpu_percent","ram_percent"], index="elapsed_seconds")
print(melted)

g = sns.FacetGrid(melted, row="variable", sharey=False, aspect=2)
g.map(sns.lineplot, "elapsed_seconds", "value")
plt.savefig("seaborn_plot.png")