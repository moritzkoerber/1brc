#! /usr/bin/env python3
import sys

import polars as pl

try:
    _, path = sys.argv
except ValueError:
    print("Usage: python solve_polars.py <path-to-measurements>")
    sys.exit(1)

print(
    "{"
    + ", ".join(
        pl.scan_csv(
            path,
            separator=";",
            has_header=False,
            new_columns=["station", "value"],
        )
        .group_by("station")
        .agg(
            pl.min("value").alias("min"),
            pl.mean("value").round(1).alias("mean"),
            pl.max("value").alias("max"),
        )
        .sort("station")
        .with_columns(
            pl.concat_str(
                pl.col("station"),
                pl.lit("="),
                pl.col("min"),
                pl.lit("/"),
                pl.col("mean"),
                pl.lit("/"),
                pl.col("max"),
            ).alias("formatted")
        )
        .select("formatted")
        .collect()["formatted"]
        .to_list()
    )
    + "}"
)
