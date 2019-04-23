import argparse
import glob
import os
import json
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Split the given dataset into mini-batches')
parser.add_argument('input_path', metavar='<input path to dataset>', type=str, help='Files to split')
parser.add_argument('output_path', metavar='<output path>', type=str, help='Path to save the output')

parser.add_argument('--rownums', metavar='N', type=int, help='Number of rows in each mini-batch')
parser.add_argument('--skip-header', dest='skip_header', action='store_const', const=True, default=False, help='skip the header')
args = parser.parse_args()

def write(oid, buffer):    
    output_path = os.path.join(args.output_path, f'{oid}.csv')
    with open(output_path, 'w', encoding='utf8') as fout:
        for line in buffer:
            print(line, file=fout)
            
    return output_path
    
def main():
    oid = 0
    header = None
    metadata = {
        'input_files': [],
        'output_files': [],
        'header': None
    }

    for input_index, input_path in enumerate(tqdm(glob.glob(args.input_path))):
        metadata['input_files'].append(input_path)

        with open(input_path) as fin:
            count = 0
            buffer = []
            for i, line in enumerate(fin.readlines()):
                if args.skip_header and i == 0:
                    if header is None:
                        header = line

                    continue
                
                count += 1
                buffer.append(line)

                if count % args.rownums == 0:
                    output_path = write(oid, buffer)
                    metadata['output_files'].append({
                        'input_index': input_index,
                        'input_path': input_path,
                        'output_index': oid,
                        'output_path': output_path,
                        'num_rows': len(buffer)
                    })

                    oid += 1
                    buffer = []

        if len(buffer) > 0:
            output_path = write(oid, buffer)
            metadata['output_files'].append({
                'input_index': input_index,
                'input_path': input_path,
                'output_index': oid,
                'output_path': output_path,
                'num_rows': len(buffer)
            })
            oid += 1

        if header is not None:
            metadata['header'] = [{"name": name} for name in header.strip().split(',')]
        
        with open(os.path.join(args.output_path, 'metadata.json'), 'w', encoding='utf8') as outf:
            json.dump(metadata, outf, indent=2)           

        
if __name__ == '__main__':
    if args.rownums is None:
        raise Exception('--rownums must be given')

    main()
