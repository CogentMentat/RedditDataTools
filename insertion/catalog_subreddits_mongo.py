# coding: utf-8
"""
Catalog subreddits found in a submission collection into a new collection
associating subreddit_id and subreddit fields, with .

Author: Alexander TJ Barron
Date Created: 2018-03-05

"""

import pdb

import argparse

from pymongo import MongoClient

def catalog_subreddits_mongo(dbname, submissioncollname):

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

    # Check that the subreddit_id field has an index.
    assert "subreddit_id_1" in submcoll.index_information(), \
            "subreddit_id index not present on submission collection"

    # We can catalog all `subreddit_id` instances, their associated subreddit
    # names (`subreddit`) (sometimes the subreddit is the same, so has the
    # same id, but the name changes), and the number of submissions in the
    # subreddit using mongodb's aggregate function.  Using the `$out` operator
    # also inserts the results of aggregate into a new collection.
    groupstage = {"$group": {"_id": "$subreddit_id",
                             "subreddit": {"$addToSet": "$subreddit"},
                             "submission_count": {"$sum": 1}}}
    outstage = {"$out": insertcollname}
    # aggregate's memory usage is capped, so we set a flag to allow disk usage
    # if necessary.
    commandcursor = submcoll.aggregate([groupstage, outstage],
                                       allowDiskUse=True)

    insertcoll = db[insertcollname]
    insertcoll.create_index("subreddit")

def main(dbname, submissioncollname):

    catalog_subreddits_mongo(dbname, submissioncollname)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('dbname', type=str,
            help="DB containing submission collection.")
    parser.add_argument('submissioncollname', type=str,
            help="Submission collection name.")

    args = parser.parse_args()

    main(args.dbname, args.submissioncollname)
