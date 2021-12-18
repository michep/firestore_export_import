from google.cloud.firestore_v1.base_query import BaseQuery
from google.cloud.firestore_v1.collection import CollectionReference
import yaml
import sys
import argparse
from typing import Any, List, Union, Dict

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1.client import Client
from google.cloud.firestore_v1.document import DocumentReference

def exportdata(db: Client, path: str) -> Dict[str, Any]:
    res : Dict[str, Any] = {}
    ref = db.document(path)
    colls : List[CollectionReference] = ref.collections


def searchquery(search: str, db: Client):
    path = search.split('/')
    path.pop(0)
    field = path.pop(-1).replace('[', '').replace(']', '')
    if len(path) % 2 == 1:
        raise Exception(
            'wrong number of elements in path - must be multiple of 2')
    doc_ref: Union[Client, DocumentReference] = db
    for coll, query in zip(path[0::2], path[1::2]):
        bq : BaseQuery = None
        for q in query.replace('[', '').replace(']', '').split('&&'):
            f, v = q.split('=')            
            bq = doc_ref.collection(coll).where(f, "==", v) if bq == None else bq.where(f, '==', v)
        res = bq.get()
        if len(res) > 0:
            id = res[0].id
            doc_ref = doc_ref.collection(coll).document(id)
        else:
            raise Exception('query result is empty', search)

    return doc_ref.id if field == '_id' else doc_ref._data[field]


def processdata(doc: Dict[str, Any], ref: Union[Client, DocumentReference]):
    db: Client = ref if type(ref) == Client else ref._client

    for coll in doc:
        print('collection', coll)
        coll_ref = ref.collection(coll)
        dd :  Dict[str, Any] = None
        for dd in doc[coll]:
            subdoc: Dict[str, Any] = None
            docref: DocumentReference = None

            for f in dd:
                v: str = dd[f]
                if type(v) == str and v.startswith('$query|'):
                    dd[f] = searchquery(v[7:], db)

                if type(v) == list:
                    for i, vv in enumerate(v):
                        if type(vv) == str and vv.startswith('$query|'):
                            v[i] = searchquery(vv[7:], db)


            if '_subcollections' in dd:
                subdoc = dd.pop('_subcollections')

            if '_id' in dd:
                id = dd.pop('_id')
                docref = coll_ref.document(id)
                if docref.get().exists:
                    if len(dd.keys()) != 0:
                        docref.update(dd)
                        print('update {' + id + '}', dd)
                else:
                    docref.set(dd)
                    print('set {' + id + '}', dd)
            else:
                docref = coll_ref.add(dd)[1]
                print('add ', dd)

            if subdoc:
                processdata(subdoc, docref)


def main():
    cmd = argparse.ArgumentParser('Import yaml data into Firestore database')
    cmd.add_argument(
        'service_file', help='google cloud service account credentials .json file')
    cmd.add_argument('data_file', help='yaml data file for import or export')
#    cmd.add_argument('--export', help='perform export, if not present - import will be performed')
    if len(sys.argv) == 1:
        args = vars(cmd.parse_args(
        ['D:\\Projects\\schoosch-8e6d4-firebase-adminsdk-qtszm-5fcb843461.json', 'd:\\Projects\\schoosch\data\people.yml']))
    else:
        args = vars(cmd.parse_args())

    cred = credentials.Certificate(args['service_file'])
    firebase_admin.initialize_app(cred)


    db: Client = firestore.client()

    with open(args['data_file'], encoding='utf8') as data_file:
#        if args['--export']:
#            export = exportdata(db)
#            file = open(data_file)
#            yaml.dump(export, file)
#            
#        else:
            doc: Dict[str, Any] = yaml.safe_load(data_file)

    processdata(doc, db)


if __name__ == '__main__':
    sys.exit(main())
