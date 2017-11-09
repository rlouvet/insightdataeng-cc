#!/usr/bin/env python
# First version of the project for Insight Data Engineering Coding Challenge
# This version is not handling advanced stream processing, it is just reading from
# one pipe-separated flat file.
# The goal is only to have a first non-optimal solution then build from it.

# Imports
import sys, os
import csv, json
import collections
import re
import helper as hl
import bufferproc as bp
# IMPROVEMENT: Maybe using Pandas library could be good to ease aggregations computation (i.e. sum over ZIP)
# however the "append row to dataframe" seems not very performant compared to a Python native data structure

# === Parameters
write_freq = 500000  # One output write every N records, should be dimensionned according to your machine
zip_pattern = re.compile(r"^[0-9]{5}(?:[0-9]{4})?$")

# === Reading input and output paths ===

# ASSUMPTION: these path are not empty and lead to valid files
input_path, output_zip_path, output_date_path = str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3])

# Getting file lenght (total lines count)
linecount = hl.line_count(input_path)


buffer_list = []
buffer_dict, zip_dict, date_dict = collections.OrderedDict(), collections.OrderedDict(), collections.OrderedDict()

# === Looping over the input file ===

with open(input_path, 'r') as inputf:

	for line_num, line in enumerate(inputf):
		#print("Processing line: " + str(line_num))

		#ASSUMPTION: we assume that each line has the expected format
		#i.e. 21 pipe-separated values with right type (int / str) and right mapping
		line = line.split('|')
		CMTE_ID, ZIP_CODE, TRANSACTION_DT, TRANSACTION_AMT, OTHER_ID = \
			line[0], line[10], line[13], line[14], line[15]
		
		# === Inuput fields validation

		# Detecting invalid ZIP code field content, but this is not triggering record-skip
		if zip_pattern.match(ZIP_CODE) == None:
			FLAG_VALID_ZIPCODE = False
		else:
			FLAG_VALID_ZIPCODE = True

		# Detecting invalid TRANSACTION_DT field content, but this is not triggering record-skip
		FLAG_VALID_TRANSACTION_DT = hl.valid_date(TRANSACTION_DT)

		# Detecting if both ZIP code and date are invalid
		ZIP_AND_DATE_INVALID = (not FLAG_VALID_ZIPCODE) and (not FLAG_VALID_TRANSACTION_DT)

		# if critical fields are empty or OTHER_ID not empty then skip the record
		if CMTE_ID == '' or TRANSACTION_AMT == '' or (not (OTHER_ID == '')) or (ZIP_AND_DATE_INVALID == True):
			if (line_num == (linecount - 1)) and len(buffer_list) > 1:
				bp.process_buffer(buffer_list, zip_dict, date_dict, output_zip_path, output_date_path)

				# Empy buffer
				buffer_dict = {}
				buffer_list = []
			
			continue

		buffer_dict = {'CMTE_ID': str(CMTE_ID), 'ZIP_CODE': str(ZIP_CODE[:5]), \
		'TRANSACTION_DT':str(TRANSACTION_DT), 'TRANSACTION_AMT':int(TRANSACTION_AMT), 'OTHER_ID':OTHER_ID,\
		'FLAG_VALID_ZIPCODE': FLAG_VALID_ZIPCODE, 'FLAG_VALID_TRANSACTION_DT': FLAG_VALID_TRANSACTION_DT}

		buffer_list.append(buffer_dict)

		#TODO: Make use of the flag FLAG_INVALID_ZIPCODE


		# IMPROVEMENT: Once in a while or at the end of the file perform the aggregations
		# and clean the buffer variables to avoid memory overflow
		if line_num == (linecount - 1) or line_num % write_freq == 0:
			bp.process_buffer(buffer_list, zip_dict, date_dict, output_zip_path, output_date_path)

			# Empy buffer
			buffer_dict = {}
			buffer_list = []
