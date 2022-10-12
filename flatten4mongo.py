from datetime import datetime
from typing import Any, List, Dict
import yaml
import sys
import argparse
import json
import bson

class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True

def flatten(data: Dict[str, Any], res: Dict[str, Any], parentname: List[str], parentid: List[str], all_ids: bool) -> Dict[str, Any]:
    for coll in data:
        print('collection', coll)
        records = data[coll]
        if not coll in res:
            res[coll] = []
        for record in records:
            if len(parentname) > 0 and len(parentid) > 0 and len(parentname) == len(parentid):
                for i in range(len(parentname)):
                    record[parentname[i] + '_id'] = parentid[i]
            sub = {}
            if '_subcollections' in record:
                sub = record.pop('_subcollections')
            for i in range(len(record)):
                key: str = list(record.keys())[i]
                if type(record[key]) == str and len(record[key]) == 20 and (key.endswith('_id') or key.endswith('_by')):
                    record[key] = { '$oid': bson.ObjectId(bytes(record[key][:12], 'utf-8')) }
                if type(record[key]) == list and key.endswith('_ids'):
                    for j in range(len(record[key])):
                        if type(record[key][j]) == str and len(record[key][j]) == 20:
                            record[key][j] = { '$oid': bson.ObjectId(bytes(record[key][j][:12], 'utf-8')) }
                if type(record[key]) == datetime:
                    if record[key].isoformat()[11:19] == '00:00:00':
                        record[key] = { '$date': record[key].isoformat()[:-6]+'+00:00' }
                    else:
                        record[key] = { '$date': record[key].isoformat()[:-6]+'-03:00' }

            res[coll].append(record)
            if len(sub) > 0:
                if all_ids:
                    flatten(sub, res, [coll, *parentname], [record['_id'], *parentid], all_ids)
                else:
                    flatten(sub, res, [coll], [record['_id']], all_ids)


def main():
    cmd = argparse.ArgumentParser(description='Flatten Firestore database export data')
    cmd.add_argument('input_file', help = 'yaml file to process')
    # cmd.add_argument('output_file', help = 'Output json file')
    cmd.add_argument('-a', '--allids', help='include full path references in subclooections docutemns', action='store_true')
    cmd.add_argument('-m', '--mongo', help='dump collections to separate files with ready-to-import format', action='store_true')


    args = vars(cmd.parse_args())

    input_file = args['input_file']
    in_path = input_file.split('.')
    in_path[1] = 'json'
    output_file = '.'.join(in_path)


    with open(input_file, mode = 'r', encoding = 'utf8') as infile:
        data: Dict[str, Any] = yaml.safe_load(infile)
        res = {}
        flatten(data, res, [], [], args['allids'])
        if args['mongo']:
            for k in res:
                with open('{file}.json'.format(file = k), mode = 'w+', encoding = 'utf-8') as outputfile:
                    for o in res[k]:
                        json.dump(o, outputfile, indent = 2, default = str, ensure_ascii = False)
                        outputfile.write('\n')
        else:
            with open(output_file, mode = 'w+', encoding = 'utf8') as outputfile:
                # yaml.dump(res, outputfile, allow_unicode = True, indent = 2, Dumper=NoAliasDumper)
                json.dump(res, outputfile, indent = 2, default=str, ensure_ascii=False)

if __name__ == '__main__':
    sys.exit(main())
