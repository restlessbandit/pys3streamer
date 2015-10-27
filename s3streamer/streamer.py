from __future__ import unicode_literals
from __future__ import print_function

from boto.s3.connection import S3Connection

class S3Streamer(object):

    #def __init__(self, bucket_name, *key_names, s3_connection=None, key_is_prefix=False):
    def __init__(self, bucket_name, *key_names, **kwargs):
        """Create a new S3Streamer. Automatically creates a Boto S3Connection if one is not provided.

        Args:
            bucket_name: String name of the Amazon S3 bucket. All keys read will be read from this bucket.
            key_names: List of key names to cat together and read, or list of key prefixes.
            s3_connection (optional): A Boto S3Connection object. One will be created from your env settings if not provided
            key_is_prefix (optional): Boolean determining if key_names should be interpreted as key prefixes (True means interpret as key_prefix). Note no deduplication is done for keys when multiple key prefixes are used. It is possible to get the same key multiple times if it matches multiple prefixes.
        """
        if not len(key_names):
            raise ValueError("At least one key name must be provided")
        self._key_strs = key_names
        s3_connection = kwargs.pop('s3_connection', None)
        key_is_prefix = kwargs.pop('key_is_prefix', False)
        if kwargs.keys():
            raise TypeError("unexpected keyword argument %s" % list(kwargs.keys())[0])

        self._conn = s3_connection or S3Connection()
        self._bucket_name = bucket_name
        self._bucket = None
        self._current_key_str = iter(self._key_strs)
        self._key_is_prefix = key_is_prefix
        self._tmp_iter = None
        self._cur_key = None
        self._readline_buff = b''
        self._key_names_accessed = []
        self._read_buffer_size = 1*1024*1024
        self._hit_eof = False

    @property
    def bucket(self):
        """Get the Amazon S3 boto bucket object."""
        if not self._bucket:
            self._bucket = self._conn.get_bucket(self._bucket_name)
        return self._bucket

    def __repr__(self):
        return "<%s: %s>"%(self.__class__.__name__, self._bucket_name)

    @property
    def keys_read(self):
        """List the Amazon S3 keys that have been read so far"""
        return list(self._key_names_accessed)

    @property
    def _next_key(self):
        if self._key_is_prefix:
            if not self._tmp_iter:
                try:
                    k_str = next(self._current_key_str)
                    self._tmp_iter = iter(self.bucket.list(prefix=k_str))
                except StopIteration as e:
                    return None
            try:
                k = next(self._tmp_iter)
                self._key_names_accessed.append(k.name)
                return k
            except StopIteration as e:
                self._tmp_iter = None
                return self._next_key

        else:
            try:
                k_str = next(self._current_key_str)
            except StopIteration as e:
                return None
            return self.bucket.get_key(k_str) or self._next_key

    def _select_next_key(self):
        self._cur_key = self._next_key
        self._hit_eof = False
        return self._cur_key

    @property
    def _current_key(self):
        if not self._cur_key:
            self._select_next_key()
        return self._cur_key

    def read(self, size):
        if size is 0:
            raise ValueError("size 0 unsupported because it is dangerous.")

        d = self._current_key.read(size)
        if len(d) is not size and not self._hit_eof:
            d2 = self._current_key.read(size-len(d))
            if not d2: #HIT EOF
                self._hit_eof = True
                d += b'\n'
                return d
            d += d2

        if d:
            return d

        if not self._select_next_key():
            return b''
        return self.read(size)

    def readline(self):
        while b'\n' not in self._readline_buff:
            d = self.read(self._read_buffer_size)
            if not d:
                return b''
            self._readline_buff += d
        line, _, self._readline_buff = self._readline_buff.partition(b'\n')
        return line+b'\n'

    def __iter__(self):
        while True:
            d = self.readline()
            if not d:
                break
            yield d

    def close(self):
        pass
