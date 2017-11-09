#!/usr/bin/env python
# First version of the project for Insight Data Engineering Coding Challenge
# This version is not handling advanced stream processing, it is just reading from
# one pipe-separated flat file.

# Buffer processing function: updating state variables and writing to output files
import collections
import numpy as np
import helper as hl

def process_buffer(buffer_list, zip_dict, date_dict, output_zip_path, output_date_path):
	# INPUTS:
	# buffer_list: list of all the records that have been buffered
	# zip_dict: state variable with data needed to compute the output for median_val_by_zip.txt file
	# date_dict: state variable with data needed to compute the output for median_val_by_zip.txt file
	# output_zip_path: path to write to median_val_by_zip.txt file
	# output_date_path: path to write to median_val_by_date.txt file

	# === Computing agregations ===

	# First add the records from the parsing buffer then compute and finally dump in output buffer
	zip_output_buffer_line = []
	zip_output_buffer = []
	date_output_buffer_line = []
	date_output_buffer = []

	for record in buffer_list:

		# === ZIP ===

		# Skip if ZIP code is not valid
		if record['FLAG_VALID_ZIPCODE'] == True:

			# Testing existence of the ZIP output file primary key (CMTE_ID, ZIP_CODE)
			# Using key by concatenating CMTE_ID & ZIP_CODE rather than using nested dictionnaries
			# Using collections.OrderedDict to preserve the input file records order

			zip_key = '|'.join([record['CMTE_ID'], record['ZIP_CODE']])

			if not hl.keys_exists(zip_dict, zip_key):
				zip_dict[zip_key] = {'RUN_LIST': [], 'RUN_MED': 0, 'TRA_COUNT': 0, 'TRA_SUM': 0}
			

			# Compute ZIP count and sum
			zip_dict[zip_key]['RUN_LIST'].append(record['TRANSACTION_AMT'])
			zip_dict[zip_key]['RUN_LIST'] = sorted(zip_dict[zip_key]['RUN_LIST'])
			zip_dict[zip_key]['RUN_MED'] = int(round(np.median(zip_dict[zip_key]['RUN_LIST'])))
			zip_dict[zip_key]['TRA_COUNT'] += 1
			zip_dict[zip_key]['TRA_SUM'] += record['TRANSACTION_AMT']


			# Writing to output buffer
			zip_output_buffer_line = [zip_key, zip_dict[zip_key]['RUN_MED'], zip_dict[zip_key]['TRA_COUNT'],\
			zip_dict[zip_key]['TRA_SUM']]
			zip_output_buffer.append(zip_output_buffer_line)

		# === DATE ===

		# Skip if DATE is not valid
		if record['FLAG_VALID_TRANSACTION_DT'] == True:

			# Testing existence of the DATE output file primary key (CMTE_ID, TRANSACTION_DT)
			# This time storing right-format date and computing an alphabetical-sort-friendly key
			alphabetical_sort_friendly_date = str(record['TRANSACTION_DT'][4:]) + str(record['TRANSACTION_DT'][:2]) +\
											 str(record['TRANSACTION_DT'][2:4])
			
			date_key = '|'.join([record['CMTE_ID'], alphabetical_sort_friendly_date])
			output_date_key = '|'.join([record['CMTE_ID'], record['TRANSACTION_DT']])

			if not hl.keys_exists(date_dict, date_key):
				date_dict[date_key] = {'RUN_LIST': [], 'RUN_MED': 0, 'TRA_COUNT': 0, 'TRA_SUM': 0,\
				'OUTPUT_DATE_KEY': output_date_key}


			# Compute DATE count and sum
			date_dict[date_key]['RUN_LIST'].append(record['TRANSACTION_AMT'])
			date_dict[date_key]['TRA_COUNT'] += 1
			date_dict[date_key]['TRA_SUM'] += record['TRANSACTION_AMT']
	
	
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

	return True