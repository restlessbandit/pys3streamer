s3streamer
==========

A file like object wrapper for groups of S3 files.
Boto by default supports reading the contents Amazon S3 'Keys' (files) with a file like read() function. Boto does not expose readline(), nor does it allow you to cat many files in an S3 bucket together and read the combined data out without having to manually change the key you read.

S3Streamer can be used in place of sys.stdin or other files.

Keys can be specified one at a time, or with one or more prefixes. Currently support for boto's delimiter feature for looking up keys is not supported.
