#!/usr/bin/env python2.7
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


"""Verifies and organizes Stack Exchange data dumps within a directory.
"""


import ast
import calendar
import collections
import glob
import gzip
import hashlib
import json
import os
import pprint
import re
import StringIO
import sys
import urllib2


def main(root_path='./'):    
    root_path = os.path.abspath(root_path)
    rel_path = lambda *a: os.path.join(root_path, *a)

    all_files = []
    for dirpath, dirnames, filenames in os.walk(root_path):
        for filename in filenames:
            all_files.append(os.path.join(dirpath, filename))

    releases_descriptions = collections.OrderedDict(
        (release.infohash, release) for release in ReleaseDescription.fetch_list())

    release_infos = {}
    release_metainfo_paths = {}

    for path in all_files:
        pp(path)

        data = try_bdecode(open(path, 'rb').read())
        if data and 'info' in data:
            info = data['info']
            try:
                info_encoded = bencode(info)
            except SyntaxError as ex:
                print(ex)
                continue
            infohash = hashlib.sha1(info_encoded).hexdigest()

            if infohash in release_infos:
                print("Duplicate! Duplicate! %r", path)

            release_infos[infohash] = info
            release_metainfo_paths[infohash] = path
    
    for infohash in release_metainfo_paths:
        
        if infohash in releases_descriptions:
            is_release = True
            description = releases_descriptions[infohash]
        else:
            is_release = False

        info = release_infos[infohash]
        path = release_metainfo_paths[infohash]

        if 'length' in info:
            length = info['length']
        else:
            length = sum(f['length'] for f in info['files'])
        name = info['name']

        if is_release:
            prefix_path = ''
        else:
            prefix_path = 'unrecognized-torrents'

        target_path = rel_path(prefix_path, '-'.join([
            '%08x' % length,
            re.sub(r'[^0-9a-z]+', '_', name.lower()).strip('_') or 'unknown',
            infohash
        ]), 'metainfo.torrent')

        os.renames(path, target_path)


def try_bdecode(b):
    """Bencoding-decodes bytes into a JSON-like structure.

    Returns None if by is not a valid bencoded value.
    """

    return bdecode(b)

    try:
        result = bdecode(b)
        if isinstance(result, dict):
            return result
    except SyntaxError:
        pass



def pp(*things):
    for thing in things:
        pprint.pprint(thing)


"""
Output?

/missing.txt
/${4-byte hex encoding of file size}-${torrent name}-${4-byte prefix of infohash}/
        metainfo.torrent
        data/${torrent name}*
unknown/* # everything else thrown in here, preserving rest of relative path.

00ffffff-e73b7025-stackexchange/e73b7025a2af72124ae49d184fa3e8cec3f66016.torrent
e73b70-stackexchange/e73b7025a2af72124ae49d184fa3e8cec3f66016.torrent


"""


class ReleaseDescription(collections.namedtuple('ReleaseDescription', '''
    infohash
    year
    month
    associated_url
    special_label
''')):
    @classmethod
    def fetch_list(ReleaseDescription):
        """Fetches and returns a list of all releases from the list on meta.
        """

        post_url = (
                'http://api.stackexchange.com/2.2'
                '/posts/224922?site=meta&filter=3r)h4BbD4n6jy2p9OfQX1'
                '&key=')
        post_response = urllib2.urlopen(post_url)
        post_response_body = gzip.GzipFile(fileobj=StringIO.StringIO(post_response.read())).read()
        post_data = json.loads(post_response_body)
        post_source = post_data['items'][0]['body_markdown']

        release_arg_codes = re.findall(r'new Release(\([^\)]*\))', post_source, re.DOTALL)

        releases = []

        lower_month_names = [name.lower() for name in calendar.month_name]

        for release_declaration in release_arg_codes:
            release_args = () + ast.literal_eval(release_declaration)
            infohash = '' + release_args[0]
            year = 0 + release_args[1]
            month_name = '' + release_args[2]
            month = lower_month_names.index(month_name.lower())

            if release_args[3:]:
                associated_url = '' + release_args[3]
            else:
                associated_url = None

            if release_args[4:]:
                special_label = '' + release_args[4]
            else:
                special_label = None

            if release_args[5:]:
                raise ValueError("got too many arguments, extras == %r" % release_args[5:])

            releases.append(ReleaseDescription(infohash, year, month, associated_url, special_label))

        return releases





def bdecode(x):
    def decode_int(x, f):
        f += 1
        newf = x.index(b'e', f)
        n = int(x[f:newf])
        if x[f] == b'-':
            if x[f + 1] == b'0':
                raise SyntaxError("got %r expecting %r" % (x[f + 1], b'0'))
        elif x[f] == b'0' and newf != f+1:
            raise SyntaxError("got invalid leading zero in integer")
        return (n, newf+1)

    def decode_string(x, f):
        colon = x.index(b':', f)
        n = int(x[f:colon])
        if x[f] == b'0' and colon != f+1:
            raise SyntaxError("got invalid leading zero in string length")
        colon += 1
        return (x[colon:colon+n], colon+n)

    def decode_list(x, f):
        r, f = [], f+1
        while x[f] != b'e':
            v, f = decode_func[x[f]](x, f)
            r.append(v)
        return (r, f + 1)

    def decode_dict(x, f):
        r, f = {}, f+1
        while x[f] != b'e':
            k, f = decode_string(x, f)
            r[k], f = decode_func[x[f]](x, f)
        return (r, f + 1)

    decode_func = {}
    decode_func[b'l'] = decode_list
    decode_func[b'd'] = decode_dict
    decode_func[b'i'] = decode_int
    decode_func[b'0'] = decode_string
    decode_func[b'1'] = decode_string
    decode_func[b'2'] = decode_string
    decode_func[b'3'] = decode_string
    decode_func[b'4'] = decode_string
    decode_func[b'5'] = decode_string
    decode_func[b'6'] = decode_string
    decode_func[b'7'] = decode_string
    decode_func[b'8'] = decode_string
    decode_func[b'9'] = decode_string

    r, l = decode_func[x[0]](x, 0)

    if l != len(x):
        raise SyntaxError("invalid bencoded value (data after valid prefix)")
    return r

def bencode(x):
    def encode_int(x, r):
        r.extend((b'i', str(x).encode('ascii'), b'e'))

    def encode_bool(x, r):
        if x:
            encode_int(1, r)
        else:
            encode_int(0, r)
            
    def encode_string(x, r):
        r.extend((str(len(x)).encode('ascii'), b':', x))

    def encode_list(x, r):
        r.append(b'l')
        for i in x:
            encode_func[type(i)](i, r)
        r.append(b'e')

    def encode_dict(x,r):
        r.append(b'd')
        ilist = x.items()
        ilist.sort()
        for k, v in ilist:
            r.extend((str(len(k)).encode(), b':', k))
            encode_func[type(v)](v, r)
        r.append(b'e')

    encode_func = {}
    encode_func[int] = encode_int
    encode_func[long] = encode_int
    encode_func[str] = encode_string
    encode_func[list] = encode_list
    encode_func[tuple] = encode_list
    encode_func[dict] = encode_dict
    encode_func[bool] = encode_bool

    r = []
    encode_func[type(x)](x, r)
    return b''.join(r)



if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
    