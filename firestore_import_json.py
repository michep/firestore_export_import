import json
import sys
import argparse
from typing import Any, Union, Dict

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1.client import Client
from google.cloud.firestore_v1.document import DocumentReference


def processdata(doc: Dict[str, Any], ref: Union[Client, DocumentReference]):
    for coll in doc.keys():
        coll_ref = ref.collection(coll)
        docref : DocumentReference = None
        subdoc : Dict[str, Any] = None
        for dd in doc[coll]:
            if '_subcollections' in dd:
                subdoc = dd.pop('_subcollections')
            if '_id' in dd:
                id = dd.pop('_id')
                docref = coll_ref.document(id)
                docref.set(dd)
                print('set {' + id + '}', dd)
            else:
                docref = coll_ref.add(dd)[1]
                print('add ', dd)
            if subdoc:
                processdata(subdoc, docref)

def main():
    cmd = argparse.ArgumentParser('Import json data into Firestore database')
    cmd.add_argument('service_file', help='google cloud service account credentials .json file')
    cmd.add_argument('data_file', help='.json data file for import')
    args = vars(cmd.parse_args())

    cred = credentials.Certificate(args['service_file'])
    firebase_admin.initialize_app(cred)

    db : Client = firestore.client()

    with open(args['data_file']) as json_file:
        doc : Dict[str, Any] = json.load(json_file)

    processdata(doc, db)


if __name__ == '__main__':
    sys.exit(main())
