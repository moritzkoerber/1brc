# 1brc

A pure python and polars solution to [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc). Performance on my M2 Pro (10 cores, 32 GB RAM):

| Command | Mean [s] | Min [s] | Max [s] |
|:---|---:|---:|---:|
| `python solve.py measurements-1_000_000_000.txt 10` | 45.094 ± 0.482 | 44.780 | 45.940 |
