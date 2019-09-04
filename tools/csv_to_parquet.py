import argparse
import glob
import random
import os
import json
import random
import pandas as pd
import csv

from tqdm import tqdm

parser = argparse.ArgumentParser(description='merge csv into a parquet')
parser.add_argument('input_path', metavar='<input path to dataset>', type=str, help='CSV files to filter')
parser.add_argument('metadata_path', metavar='<input path to dataset>', type=str, help='CSV files to filter')
parser.add_argument('output_path', metavar='<output path>', type=str, help='Path to save the output')

parser.add_argument('-n', metavar='N', type=int, default=30, help='how many csvs to merge')

args = parser.parse_args()

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def main():
    with open(args.metadata_path, 'r', encoding='utf8') as inf:
        metadata = json.load(inf)

    headers = [header['name'] for header in metadata['header']]

    files = list(glob.glob(args.input_path))

    random.shuffle(files)

    output_files = []

    for i, paths in tqdm(enumerate(chunks(files, args.n))):
        print(i, len(files) // args.n)

        # dfs = []

        # for path in paths:
        #     df = pd.read_csv(path, index_col=None, header=None, names=headers)
        #     # print(df.head(10))
        #     dfs.append(df)

        # df = pd.concat(dfs, axis=0, ignore_index=True, sort=True)

        # df.to_parquet(os.path.join(args.output_path, '{}.parquet'.format(i)))        

        output_files.append({
            'index': i,
            'path': '{}.parquet'.format(i),
            'num_rows': 3000000 * args.n #len(df.index)
        })
        
    with open(os.path.join(args.output_path, 'metadata.json'), 'w') as outf:
        json.dump({
            'output_files': output_files,
            'header': metadata['header']
        }, outf, indent=2)


if __name__ == '__main__':
    main()
