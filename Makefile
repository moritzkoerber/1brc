FILE ?= measurements-1_000_000_000.txt
CORES ?= 10

all: polars python

polars:
	python solve_polars.py $(FILE)

python:
	python solve.py $(FILE) $(CORES)
