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
import re
import StringIO
import sys
import urllib2


def main(root_path='./'):    
    root_path = os.path.abspath(root_path)
    rel_path = lambda *a: os.path.join(root_path, *a)

    missing = open(rel_path('missing.txt'), 'wt')
    missing.write("Missing data:\n")
    def write_missing(s):
        missing.write(s)
        sys.stderr.write(s)

    all_files = []
    for dirpath, dirnames, filenames in os.walk(root_path):
        for filename in filenames:
            all_files.append(os.path.join(dirpath, filename))

    releases_descriptions = collections.OrderedDict(
        (release.infohash, release) for release in ReleaseDescription.fetch_list())

    release_infos = {}
    release_metainfo_paths = {}

    for path in all_files:
        print(path)

        data = try_bdecode(open(path, 'rb').read())
        if data and 'info' in data:
            info = data['info']
            try:
                info_encoded = bencode(info)
            except BencodeDecodeError as ex:
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
            '%010x' % (length),
            re.sub(r'[^0-9a-z]+', '_', name.lower()).strip('_') or 'unknown',
            infohash
        ]), 'metainfo.torrent')

        os.renames(path, target_path)

    for infohash in set(releases_descriptions) - set(release_metainfo_paths):
        description = releases_descriptions[infohash]
        write_missing(" - metainfo file for %s.\n" % (description, ))


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


def try_bdecode(data):
    try:
        return bdecode(data)
    except BencodeDecodeError:
        return None


def bdecode(data):
    if not isinstance(data, bytes):
        raise TypeError("can only bdecode bytes, not %s" % (type(data),))

    def decode_any(start_index):
        first_byte = data[start_index:start_index + 1]

        if not first_byte:
            raise BencodeDecodeError("got end of data, expecting beginning of bencoded value")
        elif b'l' == first_byte:
            result, next_index = decode_list(start_index)
        elif b'd' == first_byte:
            result, next_index = decode_dict(start_index)
        elif b'i' == first_byte:
            result, next_index = decode_dict(start_index)
        elif b'0' <= first_byte <= b'9':
            result, next_index = decode_string(start_index)
        else:
            raise BencodeDecodeError("got %r expecting beginning of bencoded value" % (first_byte,))

        return result, next_index

    def decode_int(start_index):
        first_digit_index = start_index + 1
        end_index = data.index(b'e', first_digit_index)

        first_digit = data[first_digit_index]

        if first_digit == b'-':
            if data[first_digit_index + 1] == b'0':
                raise BencodeDecodeError("unexpected leading zero in negative integer")
        elif first_digit == b'0' and end_index - first_digit_index > 1:
            raise BencodeDecodeError("unexpected leading zero in integer")

        value = int(data[first_digit_index:end_index])
        next_index = end_index + 1
        return value, next_index

    def decode_string(start_index):
        colon_index = data.index(b':', start_index)

        if data[start_index] == b'0' and colon_index - start_index > 1:
            raise BencodeDecodeError("unexpected leading zero in string length")

        value_length = int(data[start_index:colon_index])

        value = data[colon_index:colon_index + value_length]

        if len(value) != value_length:
            raise BencodeDecodeError(
                "expected %s bytes for string, but only %s bytes remained" % (value_length, len(value)))

        next_index = colon_index + value_length
        return value, next_index

    def decode_list(start_index):
        child_index = start_index + 1

        value = []
        
        while data[child_index] != b'e':
            child_value, child_index = decode_any(child_index)
            value.append(child_value)

        next_index = child_index + 1
        return value, next_index

    def decode_dict(start_index):
        child_index = start_index + 1

        value = {}
        
        while data[child_index] != b'e':
            child_key, child_index = decode_string(child_index)
            child_value, child_index = decode_any(child_index)
            value[child_key] = child_value

        next_index = child_index + 1
        return value, next_index

    result, next_index = decode_any(0)

    extra = len(data) - next_index
    if extra:
        raise BencodeDecodeError(
            "unexpected extra %s bytes (starting with %r) after end of bencoded value" % (
                extra, data[next_index:next_index + 4]))

    return result


class BencodeDecodeError(ValueError):
    pass


def bencode(root_value):
    pieces = []

    def encode_any(value):
        if isinstance(value, int):
            encode_int(value)
        elif isinstance(value, bytes):
            encode_string(value)
        elif isinstance(value, list):
            encode_list(value)
        elif isinstance(value, dict):
            encode_dict(value)
        else:
            raise TypeError("cannot bencode value of this type: %r" % (value,))

    def encode_int(value):
        pieces.append(b'i%de' % (value,))

    def encode_string(value):
        pieces.append(b'%d:' % (len(value),))
        pieces.append(value)

    def encode_list(value):
        pieces.append(b'l')
        for child_value in value:
            encode_any(child_value)
        pieces.append(b'e')

    def encode_dict(value):
        pieces.append(b'd')
        value_items = value.items()
        value_items.sort()
        for child_key, child_value in value_items:
            encode_string(child_key)
            encode_any(child_value)
        pieces.append(b'e')

    encode_any(root_value)

    return b''.join(pieces)


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
    