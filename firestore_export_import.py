from datetime import datetime
from google.cloud.firestore_v1.base_document import DocumentSnapshot
from google.cloud.firestore_v1.base_query import BaseQuery
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.client import Client
from google.cloud.firestore_v1.document import DocumentReference
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from firebase_admin import credentials
from firebase_admin import firestore
from typing import Any, List, Union, Dict
import yaml
import sys
import argparse

import firebase_admin

def exportdata(map: Dict[str, Any], ref: Union[Client, DocumentReference], noid: bool):
    collRefs: List[CollectionReference] = list(ref.collections())

    for collRef in collRefs:
        print(collRef.id)
        map[collRef.id] = []
        docs: List[DocumentSnapshot] = collRef.get()
        for doc in docs:
            dict = doc.to_dict()
            if not noid:
                dict['_id'] = doc.id
            for k in dict.keys():
                if type(dict[k]) == DatetimeWithNanoseconds:
                    dict[k] = datetime.fromtimestamp(dict[k].timestamp())
            print(dict)
            map[collRef.id].append(dict)
            a = map[collRef.id][-1]
            if len(list(doc.reference.collections())) > 0:
                a['_subcollections'] = {}
                exportdata(a['_subcollections'], doc.reference, noid)


        

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


def importdata(doc: Dict[str, Any], ref: Union[Client, DocumentReference]):
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
                importdata(subdoc, docref)


def main():
    cmd = argparse.ArgumentParser('Firestore database export and import')
    cmd.add_argument('service_file', help='google cloud service account .json file')
    cmd.add_argument('data_file', help='yaml data file for export or import')
    cmd.add_argument('-e', '--export', help='perform export, if not present - import will be performed', action='store_true')
    cmd.add_argument('-n', '--noid', help='do not export document ids', action='store_true')
    # if len(sys.argv) == 1:
    #     args = vars(cmd.parse_args(
    #     ['D:\\Projects\\schoosch-8e6d4-firebase-adminsdk-qtszm-4352033692.json', 'd:\\Projects\\schoosch\data\people123.yml', '--export']))
    # else:
    args = vars(cmd.parse_args())

    cred = credentials.Certificate(args['service_file'])
    firebase_admin.initialize_app(cred)


    db: Client = firestore.client()

    if args['export']:
        map: Dict[str, Any] = {}
        exportdata(map, db, args['noid'])
        with open(args['data_file'], mode='w+', encoding='utf8') as export_file:
            yaml.dump(map, export_file, allow_unicode=True, indent=2)
    else:
        with open(args['data_file'], mode='r', encoding='utf8') as import_file:
            doc: Dict[str, Any] = yaml.safe_load(import_file)
            importdata(doc, db)

if __name__ == '__main__':
    sys.exit(main())
