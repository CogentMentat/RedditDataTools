# coding: utf-8
"""
Insert Reddit data to mongodb.

Author: Alexander TJ Barron
Date Created: 2018-02-22

"""

import os
import argparse
import bz2
import json
import re

from pymongo import MongoClient
from itertools import zip_longest

def grouper(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks or blocks, with the last chunk
    containing the remainder.

    """
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"

    arguments = [iter(iterable)]*n

    return zip_longest(*arguments, fillvalue=fillvalue)

def insert_Reddit_tomongo(dbname, collectionname, chunksize, filepaths):

    client = MongoClient()

    # Check for existing db.
    dbnames = client.database_names()
    """
    if dbname in dbnames:
        raise Exception("DB '{}' already exists.".format(dbname))
    """

    db = client[dbname]
    collection = db[collectionname]

    # Read files and inserting records.
    for fpath in filepaths:

        fname = os.path.split(fpath)[-1]
        print(fname)
        if '.' not in fname:
            raise Exception("'.' not in filename (no extension): " \
                    "{}".format(fpath))
        ext = fname.split('.')[-1]
        if ext == "bz2":

            with bz2.open(fpath) as f:
                chunk_gen = grouper(f, chunksize)
                for chunk in chunk_gen:
                    json_recorddicts = []
                    for line in chunk:
                        if line == None: # When grouper reaches records end.
                            break

                        # Occasionally there are codec problems.
                        try:
                            jsondict = json.loads(line.strip())
                        except:
                            continue

                        # Comment files have created_utc as strings.
                        # Submission files have them as integers.
                        jsondict["created_utc"] = int(jsondict["created_utc"])

                        json_recorddicts.append(jsondict)

                    response = collection.insert_many(json_recorddicts, ordered=False)
                    del response

        else:
            raise Exception("Extension not recognized for decompression " \
                    "method.")

    # Add indexes.

    if re.match("RS", fname): # Submissions
        indexfields = ["name", "author", "subreddit_id", "created_utc"]
    elif re.match("RC", fname): # Comments
        indexfields = ["parent_id", "name", "author", "subreddit_id",
                "created_utc"]
    else:
        raise Exception("'RS' or 'RC' should start the file name.")

    for indexfield in indexfields:
        response = collection.create_index(indexfield)

def main(dbname, collectionname, chunksize, filepaths):

    insert_Reddit_tomongo(dbname, collectionname, chunksize, filepaths)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('dbname', type=str, help="Insertion DB.")
    parser.add_argument('collectionname', type=str,
            help="Insertion DB collection.")
    parser.add_argument('chunksize', type=int,
            help="Number of record lines to insert at once.")
    parser.add_argument('filepaths', type=str, nargs='+',
            help='At least one file path.  Unix * wildcards are acceptable.')

    args = parser.parse_args()

    main(args.dbname, args.collectionname, args.chunksize, args.filepaths)
