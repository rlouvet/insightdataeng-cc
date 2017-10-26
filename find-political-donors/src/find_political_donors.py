#!/usr/bin/env python
# First version of the project for Insight Data Engineering Coding Challenge
# This version is not handling advanced stream processing, it is just reading from one pipe-separated flat file
# The goal is only to have a first non-optimal solution then build from it.

# Imports
import sys, os
import json
import numpy as np
import pandas as pd
import helper

# === Reading input and output paths ===
# ASSUMPTION: these path are not empty and lead to valid files
input_path, output_zip_path, output_date_path = str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3])
print('input_path: ' + input_path)
print('output_zip_path: ' + output_zip_path)
print('output_date_path: ' + output_date_path)

mem_dict, zip_dict, date_dict = dict(), dict(), dict()
mem_df, zip_df, date_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# === Looping over the input file ===

#TO IMPROVE: Read the input file in batches and not 'all at once'
#TO IMPROVE: Is is interesting to use the 'wb' mode to write to binary txt file?

with open(input_path, 'r') as inputf, open(output_zip_path, 'w') as outputzipf,\
open(output_date_path, 'w') as outputdatef:
	for line_num, line in enumerate(inputf):
		#ASSUMPTION: we assume that each line has the expected format
		#i.e. 21 pipe-separated values with right type (int / str) and right mapping
		line = line.split('|')
		CMTE_ID, ZIP_CODE, TRANSACTION_DT, TRANSACTION_AMT, OTHER_ID = \
			line[0], line[10], line[13], line[14], line[15]
		
		if len(ZIP_CODE) < 5:
			FLAG_INVALID_ZIPCODE = True

		if CMTE_ID == '' or TRANSACTION_AMT == '' or OTHER_ID == '':

			mem_dict[line_num] = {'CMTE_ID': str(CMTE_ID), 'ZIP_CODE': str(ZIP_CODE[:5]), \
			'TRANSACTION_DT':str(TRANSACTION_DT), 'TRANSACTION_AMT':int(TRANSACTION_AMT), \
			'OTHER_ID':OTHER_ID}

			#TODO: Use the flag FLAG_INVALID_ZIPCODE

			## Testing existence of the first output file primary key (CMTE_ID, ZIP_CODE)
			#if keys_exists(zip_dict, CMTE_ID, ZIP_CODE):
			#	zip_dict[CMTE_ID][ZIP_CODE]['RUN_MED'] = zip_dict[CMTE_ID][ZIP_CODE]['RUN_MED']	
			#else:
			#	pass

			outchunk = ''
			outputzipf.write(outchunk)


print(json.dumps(mem_dict, indent = 2))