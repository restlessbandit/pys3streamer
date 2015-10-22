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


def test_single_key(s3fakeconn, keydatawithnewline):
    stream = S3Streamer('bucket', 'stuff', s3_connection=s3fakeconn)
    d_cat = cat_read(stream)
    print("DCAT", d_cat.__repr__())
    print("DATA", keydatawithnewline[0].__repr__())
    assert d_cat == keydatawithnewline[0]


def test_prefix(s3fakeconn, keydatacated):
    stream = S3Streamer('bucket', 'stuff', s3_connection=s3fakeconn, key_is_prefix=True)
    d_cat = cat_read(stream)
    print("DCAT", d_cat.__repr__())
    print("DATA", keydatacated.__repr__())
    assert d_cat == keydatacated

def test_readline(s3fakeconn, keydatacatedsplitonnewline):
    stream = S3Streamer('bucket', 'stuff', s3_connection=s3fakeconn, key_is_prefix=True)
    for l in keydatacatedsplitonnewline:
        d = stream.readline()
        print("READLINE:", d.__repr__())
        print("CORRECTL:", l.__repr__())
        assert d == l

def test_iter(s3fakeconn, keydatacatedsplitonnewline):
    stream = S3Streamer('bucket', 'stuff', s3_connection=s3fakeconn, key_is_prefix=True)
    d = list(stream)
    print("READLINE:", d.__repr__())
    print("CORRECTL:", keydatacatedsplitonnewline.__repr__())
    assert d == keydatacatedsplitonnewline
