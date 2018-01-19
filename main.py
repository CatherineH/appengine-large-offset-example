#!/usr/bin/env python

import os
from time import time

import webapp2
import json
import logging
from google.appengine.api import users, memcache
from google.appengine.api.search import search
from google.appengine.api.search.search import MAXIMUM_DOCUMENTS_RETURNED_PER_SEARCH, \
    MAXIMUM_SEARCH_OFFSET
from google.appengine.ext import blobstore, ndb
from google.appengine.api import app_identity

INDEX_NAME = "my_search_index"
NAMESPACE = "number"
DOCS_PER_PAGE_LIMIT = 100


class Initialize(webapp2.RequestHandler):
    def get(self):
        search_index = search.Index(name=INDEX_NAME, namespace=NAMESPACE)
        for i in range(3000):
            fields = [search.NumberField(name='number_value', value=i),
                      search.NumberField(name='last_modified', value=int(time()))]
            doc = search.Document(fields=fields)
            results = search_index.put(doc)


class Search(webapp2.RedirectHandler):
    def get(self):
        expr_list = [
            search.SortExpression(
                expression='number_value',
                default_value=int(0),
                direction=search.SortExpression.ASCENDING
            )
        ]
        self.sort_opts = search.SortOptions(expressions=expr_list)
        self.index = search.Index(name=INDEX_NAME, namespace=NAMESPACE)

        self.limit = int(self.request.get('limit'))
        if self.limit > MAXIMUM_DOCUMENTS_RETURNED_PER_SEARCH:
            self.limit = MAXIMUM_DOCUMENTS_RETURNED_PER_SEARCH

        self.offset = int(self.request.get('offset'))
        if self.offset < MAXIMUM_SEARCH_OFFSET:
            self.query()
        else:
            self.query_with_cursors()


    def render_search_doc(self, search_results):
        results = search_results.results
        docs = []
        for doc in results:
            docs.append([doc.doc_id, doc.fields[0].value])
        self.response.write(json.dumps(docs))

    def query(self):
        search_query = search.Query(
            query_string='',
            options=search.QueryOptions(
                limit=self.limit, sort_options=self.sort_opts, offset=self.offset, ids_only=False)
        )
        self.render_search_doc(self.index.search(search_query))

    def query_with_cursors(self):
        cursor_cache_key = 'cursors'
        cursor_cache = memcache.get(cursor_cache_key)
        if cursor_cache:
            cursor_cache = json.loads(cursor_cache)
            offset_key = str(self.offset)
            if offset_key in cursor_cache:
                cursor_cache = cursor_cache[offset_key]
            else:
                logging.info("%s not in %s" %(offset_key, cursor_cache))
                cursor_cache = None

        if cursor_cache:
            logging.info("found cursor cache string %s " % cursor_cache)

            # construct the sort options
            search_query = search.Query(
                query_string='',
                options=search.QueryOptions(
                    limit=self.limit,
                    sort_options=self.sort_opts,
                    cursor=search.Cursor(per_result=False, web_safe_string=cursor_cache),
                    ids_only=False,
                )
            )
            return self.render_search_doc(self.index.search(search_query))
        cursor_cache = {}
        current_offset = self.limit
        search_query = search.Query(query_string='',
            options=search.QueryOptions(
                limit=self.limit,
                sort_options=self.sort_opts,
                cursor=search.Cursor(per_result=False),
                ids_only=False,
            )
        )

        search_results = self.index.search(search_query)
        cursor_cache[current_offset] = search_results.cursor.web_safe_string
        if self.offset >= search_results.number_found:
            return self.render_search_doc([])
        while current_offset < self.offset:
            current_offset += self.limit
            search_query = search.Query(query_string='',
                options=search.QueryOptions(
                    limit=self.limit,
                    sort_options=self.sort_opts,
                    cursor=search_results.cursor
                )
            )
            search_results = self.index.search(search_query)
            cursor_cache[current_offset] = search_results.cursor.web_safe_string
        memcache.set(cursor_cache_key, json.dumps(cursor_cache))
        self.render_search_doc(search_results)


app = webapp2.WSGIApplication([
    ('/init', Initialize),
    ('/search', Search)
], debug=True)
