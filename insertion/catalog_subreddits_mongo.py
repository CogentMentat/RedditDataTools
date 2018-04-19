# coding: utf-8
"""
Insert Reddit data to mongodb.

Author: Alexander TJ Barron
Date Created: 2018-02-22

"""

import pdb

import argparse

from pymongo import MongoClient

def main(dbname, submissioncollname):

    client = MongoClient()

    # Check for existing db.
    dbnames = client.database_names()
    """
    if dbname in dbnames:
        raise Exception("DB '{}' already exists.".format(dbname))
    """

    db = client[dbname]
    submcoll = db[submissioncollname]

    insertcollname = "SubredIdMap"

    # Check that insertion collection doesn't exist.
    assert insertcollname not in db.collection_names(), \
            "{} collection already in db.".format(insertcollname)

    # Read files and inserting records.
    subredid_subreddit = set()
    for rec in submcoll.find({}, {"subreddit":1, "subreddit_id":1}):

        try:
            subreddit = rec["subreddit"]
            subredid = rec["subreddit_id"]
        except KeyError:
            continue

        subredid_subreddit.add((subredid, subreddit))

    # Count subreddit submissions
    subredid_counts = [(sid, submcoll.find({'subreddit_id':sid}).count()) \
            for sid, sd in subredid_subreddit]
    d_subredid_cnt = dict(subredid_counts)

    insert_records = [{"subreddit_id": sid,
                       "subreddit": sd,
                       "submission_count": d_subredid_cnt[sid]} \
                      for sid, sd in subredid_subreddit]

    # Insert to db.

    insertcoll = db[insertcollname]
    response = insertcoll.insert_many(insert_records,
                                      ordered=False)
    insertcoll.create_index("subreddit_id")
    insertcoll.create_index("subreddit")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('dbname', type=str,
            help="DB containing submission collection.")
    parser.add_argument('submissioncollname', type=str,
            help="Submission collection name.")

    args = parser.parse_args()

    main(args.dbname, args.submissioncollname)
