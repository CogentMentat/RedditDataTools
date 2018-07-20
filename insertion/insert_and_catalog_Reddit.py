# coding: utf-8
"""
Convenience script combining Reddit data insertion to mongodb and cataloging
all subreddits.

Author: Alexander TJ Barron
Date Created: 2018-04-19

"""

import argparse

from insert_Reddit_tomongo import insert_Reddit_tomongo
from catalog_subreddits_mongo import catalog_subreddits_mongo

def main(dbname, submissioncollectionname, commentcollectionname,
        subredditmapname, chunksize, submissionfilepaths, commentfilepaths):

    insert_Reddit_tomongo(dbname, submissioncollectionname, chunksize,
            submissionfilepaths)
    insert_Reddit_tomongo(dbname, commentcollectionname, chunksize,
            commentfilepaths)
    catalog_subreddits_mongo(dbname, submissioncollectionname,
            commentcollectionname, subredditmapname)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('dbname', type=str, help="Insertion DB.")
    parser.add_argument('submissioncollectionname', type=str,
            help="Submission DB collection name for insertion.")
    parser.add_argument('commentcollectionname', type=str,
            help="Comment DB collection name for insertion.")
    parser.add_argument('subredditmapname', type=str,
            help="Name of resulting subreddit map collection.")
    parser.add_argument('chunksize', type=int,
            help="Number of record lines to read/insert at once.")
    parser.add_argument('--submissionfilepaths', '-sfp', type=str, nargs='+',
            required=True,
            help='At least one submission file path.  Unix * wildcards ' \
                    'are acceptable.')
    parser.add_argument('--commentfilepaths', '-cfp', type=str, nargs='+',
            required=True,
            help='At least one comment file path.  Unix * wildcards are ' \
                    'acceptable.')

    args = parser.parse_args()

    main(args.dbname, args.submissioncollectionname,
            args.commentcollectionname, args.subredditmapname, args.chunksize,
            args.submissionfilepaths, args.commentfilepaths)
