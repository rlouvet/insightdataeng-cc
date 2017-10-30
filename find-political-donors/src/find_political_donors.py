#!/usr/bin/env python
# First version of the project for Insight Data Engineering Coding Challenge
# This version is not handling advanced stream processing, it is just reading from
# one pipe-separated flat file.
# The goal is only to have a first non-optimal solution then build from it.

# Imports
import sys, os
import csv, json
import collections
import numpy as np
import helper as hl
# IMPROVEMENT: Maybe using Pandas library could be good to ease aggregations computation (i.e. sum over ZIP)
# however the "append row to dataframe" seems not very performant compared to a Python native data structure

# === Reading input and output paths ===

# ASSUMPTION: these path are not empty and lead to valid files
input_path, output_zip_path, output_date_path = str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3])

# Getting file lenght (total lines count)
linecount = hl.line_count(input_path)

# One output write every N records
write_freq = 500000


buffer_list = []
buffer_dict, zip_dict, date_dict = collections.OrderedDict(), collections.OrderedDict(), collections.OrderedDict()

# === Looping over the input file ===

with open(input_path, 'r') as inputf:

	for line_num, line in enumerate(inputf):
		print("Processing line: " + str(line_num))

		#ASSUMPTION: we assume that each line has the expected format
		#i.e. 21 pipe-separated values with right type (int / str) and right mapping
		line = line.split('|')
		CMTE_ID, ZIP_CODE, TRANSACTION_DT, TRANSACTION_AMT, OTHER_ID = \
			line[0], line[10], line[13], line[14], line[15]
		
		# Detecting invalid ZIP code field content, but this is not triggering record-skip
		if len(ZIP_CODE) < 5:
			FLAG_INVALID_ZIPCODE = True

		# if critical fields are empty or OTHER_ID not empty then skip the record
		if CMTE_ID == '' or TRANSACTION_AMT == '' or not (OTHER_ID == ''):
			continue

		buffer_dict = {'CMTE_ID': str(CMTE_ID), 'ZIP_CODE': str(ZIP_CODE[:5]), \
		'TRANSACTION_DT':str(TRANSACTION_DT), 'TRANSACTION_AMT':int(TRANSACTION_AMT), \
		'OTHER_ID':OTHER_ID}

		buffer_list.append(buffer_dict)

		#TODO: Make use of the flag FLAG_INVALID_ZIPCODE


		# IMPROVEMENT: Once in a while or at the end of the file perform the aggregations
		# and clean the buffer variables to avoid memory overflow
		if line_num == (linecount - 1) or line_num % write_freq == 0:


			# === Computing agregations === TODO: Watchout for the indentation here!
			avancement = int(float(line_num) / float(linecount) * 100)
			print(">>(" + str(avancement) + "%)>>Computing agregations")
			print(">>current buffer_list size is: " + str(len(buffer_list)))

			# First add the records from the parsing buffer then compute and finally dump in output buffer
			zip_output_buffer_line = []
			zip_output_buffer = []
			date_output_buffer_line = []
			date_output_buffer = []

			for record in buffer_list:

				# Testing existence of the ZIP output file primary key (CMTE_ID, ZIP_CODE)
				# Using key by concatenating CMTE_ID & ZIP_CODE rather than using nested dictionnaries
				# Using collections.OrderedDict to preserve the input file records order

				zip_key = '|'.join([record['CMTE_ID'], record['ZIP_CODE']])

				if not hl.keys_exists(zip_dict, zip_key):
					zip_dict[zip_key] = {'RUN_LIST': [], 'RUN_MED': 0, 'TRA_COUNT': 0, 'TRA_SUM': 0}

				# Testing existence of the DATE output file primary key (CMTE_ID, TRANSACTION_DT)
				# This time storing right-format date and computing an alphabetical-sort-friendly key
				alphabetical_sort_friendly_date = str(record['TRANSACTION_DT'][4:]) + str(record['TRANSACTION_DT'][:2]) +\
												 str(record['TRANSACTION_DT'][2:4])
				
				date_key = '|'.join([record['CMTE_ID'], alphabetical_sort_friendly_date])
				output_date_key = '|'.join([record['CMTE_ID'], record['TRANSACTION_DT']])

				if not hl.keys_exists(date_dict, date_key):
					date_dict[date_key] = {'RUN_LIST': [], 'RUN_MED': 0, 'TRA_COUNT': 0, 'TRA_SUM': 0,\
					'OUTPUT_DATE_KEY': output_date_key}
				

				# Compute ZIP count and sum
				zip_dict[zip_key]['RUN_LIST'].append(record['TRANSACTION_AMT'])
				zip_dict[zip_key]['RUN_LIST'] = sorted(zip_dict[zip_key]['RUN_LIST'])
				zip_dict[zip_key]['RUN_MED'] = int(round(np.median(zip_dict[zip_key]['RUN_LIST'])))
				zip_dict[zip_key]['TRA_COUNT'] += 1
				zip_dict[zip_key]['TRA_SUM'] += record['TRANSACTION_AMT']

				# Compute DATE count and sum
				date_dict[date_key]['RUN_LIST'].append(record['TRANSACTION_AMT'])
				date_dict[date_key]['TRA_COUNT'] += 1
				date_dict[date_key]['TRA_SUM'] += record['TRANSACTION_AMT']

				# Writing to output buffer
				zip_output_buffer_line = [zip_key, zip_dict[zip_key]['RUN_MED'], zip_dict[zip_key]['TRA_COUNT'],\
				zip_dict[zip_key]['TRA_SUM']]
				zip_output_buffer.append(zip_output_buffer_line)

			# Empy buffer
			buffer_dict = {}
			buffer_list = []
			
			date_dict = collections.OrderedDict(sorted(date_dict.items()))

			for date_key in date_dict:
				date_dict[date_key]['RUN_LIST'] = sorted(date_dict[date_key]['RUN_LIST'])
				date_dict[date_key]['RUN_MED'] = int(round(np.median(date_dict[date_key]['RUN_LIST'])))
				date_output_buffer_line = [date_dict[date_key]['OUTPUT_DATE_KEY'], date_dict[date_key]['RUN_MED'], date_dict[date_key]['TRA_COUNT'],\
				date_dict[date_key]['TRA_SUM']]
				date_output_buffer.append(date_output_buffer_line)


			# === Writing the output files ===

			# BESTPRACTICE: Limiting the file I/O by using an output memory buffer
			# IMPROVEMENT: Is is interesting to use the 'b' mode to write to binary txt file?
			with open(output_zip_path, 'a') as outputzipf:
				for zip_record in zip_output_buffer:
					outputzipf.write('|'.join(str(val) for val in zip_record))
					outputzipf.write('\r\n')

			with open(output_date_path, 'w') as outputdatef:
				for date_record in date_output_buffer:
					outputdatef.write('|'.join(str(val) for val in date_record))
					outputdatef.write('\r\n')
