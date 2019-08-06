import argparse
import glob
import os
import json
import random
import pandas as pd
import csv

from tqdm import tqdm

parser = argparse.ArgumentParser(description='Leave specfic columns in csv')
parser.add_argument('input_path', metavar='<input path to dataset>', type=str, help='CSV files to filter')
parser.add_argument('output_path', metavar='<output path>', type=str, help='Path to save the output')

parser.add_argument('--fields', metavar='A,B,C,D', type=str, default='', help='Fields to use')
parser.add_argument('--logs', metavar='<path>', type=str, default='filter.log', help='Log file')
parser.add_argument('--remove', dest='remove', action='store_true', default=False, help='remove the original csv after filter')

args = parser.parse_args()
        
def main():
    logfile = open(args.logs, mode='a', encoding='utf8')

    for input_path in tqdm(glob.glob(args.input_path)):
        csv_name = os.path.basename(input_path)
        tqdm.write(csv_name)
        csv = pd.read_csv(input_path)
            
        header = args.fields.split(',')
        csv = csv[header]

        csv.to_csv(os.path.join(args.output_path, csv_name), index=False)
        

        if args.remove:
           size = os.stat(input_path).st_size
           print(f'{csv_name},{size}', file=logfile)
           os.remove(input_path)

    logfile.close()

if __name__ == '__main__':
    main()
