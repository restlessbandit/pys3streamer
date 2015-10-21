import sys, os
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

import pytest

from s3streamer import S3Streamer


def test_single_key(s3fakeconn, keydata):
    stream = S3Streamer('bucket', 'stuff', s3_connection=s3fakeconn)
    d_cat = ""
    while(True):
        d = stream.read(1000)
        if not d:
            break
        d_cat += d
    assert d_cat == keydata[0]


def test_prefix(s3fakeconn, keydata):
    stream = S3Streamer('bucket', 'stuff', s3_connection=s3fakeconn, key_is_prefix=True)
    d_cat = ""
    while(True):
        d = stream.read(1000)
        if not d:
            break
        d_cat += d
    print("DCAT", d_cat.__repr__())
    print("KEYS", "".join(keydata).__repr__())
    assert d_cat == "".join(keydata)
