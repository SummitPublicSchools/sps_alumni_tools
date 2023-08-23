#!python3
'''
Script for taking the result of merge_nsc.py and adding
'ENROLLMENT_STATUS' to the enrollment files
'''

import pandas as pd

match_table = pd.read_csv('debugging_output/__match_table.csv')


if __name__ == '__main__':
    print(match_table)