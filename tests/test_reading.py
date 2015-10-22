import sys, os
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

import pytest

from s3streamer import S3Streamer

def cat_read(stream):
    reads = []
    while(True):
        d = stream.read(1000)
        if not d:
            break
        reads.append(d)
    return "".join(reads)

def test_incorrect_parameters(s3fakeconn):
    with pytest.raises(TypeError):
        S3Streamer('bucket', 'stuff',
                   s3_connection=s3fakeconn, #just make sure no connect
                   incorrect_key="Test")


def test_single_key(s3fakeconn, keydata):
    stream = S3Streamer('bucket', 'stuff', s3_connection=s3fakeconn)
    d_cat = cat_read(stream)
    assert d_cat == (keydata[0]+"\n")


def test_prefix(s3fakeconn, keydata):
    stream = S3Streamer('bucket', 'stuff', s3_connection=s3fakeconn, key_is_prefix=True)
    d_cat = cat_read(stream)
    keys = "\n".join(keydata+[''])
    print("DCAT", d_cat.__repr__())
    print("KEYS", keys.__repr__())
    assert d_cat == keys
