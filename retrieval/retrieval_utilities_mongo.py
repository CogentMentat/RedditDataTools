# coding: utf-8
"""
Utilities for working with Reddit data.

Author: Alexander TJ Barron
Date Created: 2018-04-03

"""

import pdb

from itertools import zip_longest, takewhile
from datetime import datetime
from collections import deque

class RedditGetter(object):

    def __init__(self, client):
        """
        Single class for accessing a variety of slices of the Reddit data.
        Assumes that the client points to a mongo db that was filled with the
        script `insert_Reddit_tomongo.py`.

        Args:
          client: pymongo.MongoClient instance

        """

        self.client = client

    def _grouper(self, iterable, n, fillvalue=None):
        """
        Collect data into fixed-length chunks or blocks, with the last chunk
        containing the remainder.

        """
        # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"

        arguments = [iter(iterable)]*n

        for g in zip_longest(*arguments, fillvalue=fillvalue):
            yield list(takewhile(lambda x: x != fillvalue, g))

    def _make_timestamp_from_isodatetime(self, isodatestr):
        y, m, d = isodatestr.split('-')[:3]
        dtm = datetime(int(y), int(m), int(d))
        return int(dtm.timestamp())

    def get_records(self, dbname, collname, fielddict, chunksize,
            timebounds=None, projectiondict=None):
        """Get records defined by fielddict, in chunks."""

        db = self.client[dbname]
        collection = db[collname]

        query_dicts = []
        for field, val in fielddict.items():
            query_dicts.append({field:val})

        if timebounds:
            ts0 = self._make_timestamp_from_isodatetime(timebounds[0])
            ts1 = self._make_timestamp_from_isodatetime(timebounds[1])
            if collname == 'Comment':
                ts0 = str(ts0)
                ts1 = str(ts1)
            query_dicts.append({"created_utc": {"$gte":ts0}})
            query_dicts.append({"created_utc": {"$lt":ts1}})

        query_dict = {"$and": query_dicts}

        if projectiondict:
            ret = self._grouper(collection.find(query_dict, projectiondict),
                    chunksize)
        else:
            ret = self._grouper(collection.find(query_dict), chunksize)

        return ret

def get_comment_tree(root_fullname, commentcoll, desired_depth=None):
    """
    Retrieve edges, by depth from root node, of a comment tree.

    Args:
      root_fullname (str): Reddit fullname of root, example: "t3_nz3g9"
      commentcoll (pymongo Collection instance): mongo collection containing
        comments
      desired_depth (int, optional): maximum depth of tree to find, with root
        node at 0 depth

    Returns:
      dict: keys for root node name and for lists of edges for each
        incremented depth of the tree

    """

    #qroot = next(commentcoll.find({"_id": ObjectId(root_objid)}))
    #root_fullname = root["name"]

    depth_edges_list = []
    depth = 0
    dq = deque([[root_fullname]])
    while dq:

        # get children of current node depth.
        fullnames = dq.popleft()
        depth += 1
        depth_edges = []
        depth_children = []
        for fullname in fullnames:
            children = commentcoll.find({"parent_id": fullname})
            for child in children:
                depth_edges.append([fullname, child["name"]])
                depth_children.append(child["name"])
        dq.append(depth_children)
        depth_edges_list.append(depth_edges)

        if desired_depth == depth:
            break

    return {"root": root_fullname,
            "depth_edges": depth_edges_list}
