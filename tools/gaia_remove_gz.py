import argparse
import glob
import os
import json
import random
import pandas as pd
import csv

from tqdm import tqdm

parser = argparse.ArgumentParser(description='Remove gz files if they have been extracted (i.e., the corressponding csv file exists)')
parser.add_argument('input_path', metavar='<input path to csv files>', type=str, help='csv files')

args = parser.parse_args()

def main():
    for input_path in tqdm(glob.glob(args.input_path)):
        gz_path = input_path.replace('-', '_') + '.gz'

        if os.path.isfile(gz_path):
            print(f'Removing {gz_path}')
            os.remove(gz_path)
            
if __name__ == '__main__':
    main()

