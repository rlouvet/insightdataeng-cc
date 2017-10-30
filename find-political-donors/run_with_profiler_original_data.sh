#!/bin/bash
#
# Use this shell script to compile (if necessary) your code and then execute it. Below is an example of what might be found in this file if your program was written in Python
#
#python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt

python -m cProfile -o ./profiling/profile_data.pyprof ./src/find_political_donors.py ./indiv18/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt
pyprof2calltree -i ./profiling/profile_data.pyprof -o ./profiling/profile_data.pyprof.stats