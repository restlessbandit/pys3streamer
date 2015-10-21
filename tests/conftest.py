
import pytest

data = [
    "People who are really serious about software should make their own hardware. -- Alan Kay",
    "Hey, I'm a good software engineer, but I'm not exactly known for my fashion sense. White socks and sandals don't translate to 'good design sense'. -- Linux Torvalds",
    "Measuring programming progress by lines of code is like measuring aircraft building progress by weight. -- Bill Gates",
    "Object-oriented design is the roman numerals of computing. -- Rob Pike"
]

class S3KeyFaker(object):
    def __init__(self, keydata, name):
        self._keydata = keydata
        self._name = name
        self._data_offset = 0

    def read(self, count):
        print("Reading from key", self._name) 
        d = self._keydata[self._data_offset:count] if self._data_offset<len(self._keydata) else ''
        self._data_offset += count
        print("returning", d.__repr__())
        return d

    @property
    def name(self):
        return self._name

class S3BucketFaker(object):
    def __init__(self, connfaker, name):
        self._conn = connfaker
        self._name = name
        self._key_iter = iter(self._conn._data)

    def get_key(self, name):
        return S3KeyFaker(next(self._key_iter), name)

    def list(self, prefix=None):
        def key_generator():
            for i, d in enumerate(self._conn._data):
                yield S3KeyFaker(d, prefix+str(i))
        return key_generator()
    
class S3ConnectionFaker(object):
    def __init__(self, data):
        self._data = data

    def get_bucket(self, name):
        return S3BucketFaker(self, name)

@pytest.fixture(scope="module")
def s3fakeconn():
    return S3ConnectionFaker(data)
@pytest.fixture(scope="module")
def keydata():
    return data
