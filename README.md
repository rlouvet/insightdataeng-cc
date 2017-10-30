# Insight data engineering coding challenge - Oct 2017

Political donors data processing project: https://github.com/InsightDataScience/find-political-donors

## Selected best practices

- **Buffers**: used read buffer and write buffer
- **Building the string to be written**: in Python the concatenation operator (+=) seems to be less efficient than the 'join' function combined with a list comprehension. Source: https://waymoot.org/home/python_string/
- **Pandas package** could ease aggregations computation (i.e. sum over ZIP) however the "append row to dataframe" seems not very performant compared to a Python native data structure (list, dict). Source: pandas doc 
- **Profilers**: used cProfile, pyprof2calltree and KCacheGrind (Valgrind) to search for first order of magnitude in compute-intensive operations. You can get a nice profile running the following two commands.

```
./run_with_profiler.sh
kcachegrind ./profiling/profile_data.pyprof.stats
```

## Improvements if I had more time to complete the assignment

- Write more tests to cover more cases
- Use a lower-level langage than Python with better memory management (C++, Java, Scala)
- Improve the buffers management
- Write an enhanced 'median' function with cached data
- Maybe leverage numpy.memmap to create a memory mapped array