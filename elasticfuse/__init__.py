import sys
import argparse
import errno
import json
from time import time
import stat
import errno
from cachetools import LRUCache

import requests
from fuse import FUSE, Operations, LoggingMixIn, fuse_get_context

class ElasticFS(LoggingMixIn, Operations):

    es_base_url = None

    def __init__(self, es_base_url=None):
        self.fd = 0
        self.direct_io = True
        self.es_base_url = es_base_url
        self.doc_cache = LRUCache(maxsize=50, missing=self._get_document)
        self.prop_cache = LRUCache(maxsize=50, missing=self._get_property)

    # Caching methods

    def _get_document(self, url):
        req = requests.get(url)
        es_json = req.json()
        return json.dumps(es_json, indent=4)

    def _get_property(self, url):
        req = requests.get(url)
        return req.json()

    # Filesystem methods
    # ==================

    def getattr(self, path, fh=None):
        uid, gid, pid = fuse_get_context()

        path_parts = path.split('/')

        if path == '/':
            st = dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        elif len(path_parts) < 5:
            st = dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        elif len(path_parts) == 5 and path_parts[3] == "documents":
            st = dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        else:
            st = dict(st_mode=(stat.S_IFREG | 0o444), st_size=0)
            if path_parts[3] == "properties":
                es_json = self.prop_cache[self.es_base_url + '/%s/%s/_mapping' % (path_parts[1], path_parts[2])]
                mapping = json.dumps(es_json[path_parts[1]]['mappings'][path_parts[2]]['properties'][path_parts[4]]) + "\n"
                st['st_size'] = len(mapping)
            elif path_parts[3] == "documents":
                st['st_size'] = len(self.doc_cache[self.es_base_url + '/%s/%s/%s' % (path_parts[1], path_parts[2], path_parts[5])])

        st['st_ctime'] = st['st_mtime'] = st['st_atime'] = time()

        return st

    def readdir(self, path, fh):

        path_parts = []

        # Either indexes, types, docs (and potentially nested docs?)
        # if depth = 0 - indexes, 1 - types, 2 - docs, 3+ nested.

        if path == '/':
            depth = 0
        else:
            depth = len(path.split('/')) - 1
            path_parts = path.split('/')

        items = ['.', '..']

        if depth == 0:
            req = requests.get(self.es_base_url + '/_aliases')
            if req is not None:
                for index_name in req.json().keys():
                    items.append(index_name)
        elif depth == 1:
            req = requests.get(self.es_base_url + '/%s/_mapping' % path_parts[1])
            json = req.json()
            for doc_type in json[path_parts[1]]['mappings']:
                items.append(doc_type)
        elif depth == 2:
            items.append('properties')
            items.append('documents')
        elif depth == 3:
            if path_parts[3] == "properties":
                es_json = self.prop_cache[self.es_base_url + '/%s/%s/_mapping' %
                                          (path_parts[1], path_parts[2])]
                for doc_type in es_json[path_parts[1]]['mappings'][path_parts[2]]['properties']:
                    items.append(doc_type)
            elif path_parts[3] == "documents":
                items.append("0")
                items.append("10")
                items.append("20")
                items.append("30")
                items.append("40")
                items.append("50")
                items.append("60")
                items.append("70")
                items.append("80")
                items.append("90")
        elif depth == 4:
                offset = path_parts[4]
                req = requests.get(self.es_base_url + '/%s/%s/_search?from=%s&size=10' % (path_parts[1], path_parts[2], offset))
                es_json = req.json()
                for doc in es_json['hits']['hits']:
                    items.append(doc['_id'])

        for t in items:
            yield t

#    def readlink(self, path):
        # Use for index alias ?
#        return -errno.ENOSYS

    def mknod(self, path, mode, dev):
        return -errno.ENOSYS

    def rmdir(self, path):
        return -errno.ENOSYS

    def mkdir(self, path, mode):
        return -errno.ENOSYS

#    def statfs(self, path):
#        return -errno.ENOSYS

    def unlink(self, path):
        return -errno.ENOSYS

    def symlink(self, name, target):
        return -errno.ENOSYS

    def rename(self, old, new):
        return -errno.ENOSYS

    def link(self, target, name):
        return -errno.ENOSYS

    def utimens(self, path, times=None):
        return -errno.ENOSYS

    # File methods
    # ============

    def create(self, path, mode, fi=None):
        return -errno.ENOSYS

    def read(self, path, length, offset, fh):
        path_parts = path.split('/'[offset:length])
        mapping = ""

        if path_parts[3] == "properties":

            es_json = self.prop_cache[self.es_base_url + '/%s/%s/_mapping' % (path_parts[1], path_parts[2])]
            mapping = json.dumps(es_json[path_parts[1]]['mappings'][path_parts[2]]['properties'][path_parts[4]]) + "\n"
        elif path_parts[3] == "documents":
            mapping = self.doc_cache[self.es_base_url + '/%s/%s/%s' % 
                        (path_parts[1], path_parts[2], path_parts[5])]

        return mapping.encode('ascii', errors='ignore')[offset:offset+length]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("elasticsearch", help="Base URL for ES")
    parser.add_argument("directory", help="Local directory")
    args = parser.parse_args()

    fuse = FUSE(ElasticFS(args.elasticsearch), args.directory, foreground=True, ro=True)

if __name__ == '__main__':
    main()

