#!/usr/bin/python

# this program demonstrates the functionality of the versioned storage system

################################################################################
#							Database functionality
################################################################################
"""
Table format

CREATE TABLE storage (
    id int primary key autoincrement,
    hash text not null,
    version int not null,
    file blob not null
);

Vector clocks in DB are dictionaries of counters where the keys are server
names that lead the commit and the values indicate the number of times that
server has interacted with that object.

Ordering of versions in the database:
    Every time a value is updated in the database, the client must include the
    previous context of the value. the store function takes as input the leader
    of the commit process and the previous context of the variable, along with
    the file hash and value, and updates the previous context by incrementing
    the count for the leader by one before storing the (hash,version,blob) in
    the database.

    When a value is read from the database, a hash is provided and the get
    function returns a list of (clock,value) pairs for each row of the database
    sorted in descending order of occurence, that is the first row(s) (if concurrent write)
    of the response will be the highest vector clock counts and the last row(s) will be the smallest.

This means we will need only change the get protocol. We will need to send back
a whole list of responses (like peer bootstrap) instead of just 1.

There is also function in here called mergeClocks which takes two vector clocks and
returns a dict with the maximum value for each key in both clocks
i.e.  [s1:1,s2:1]+[s1:2]=[s1:2,s2:1]
"""

import hashlib
import sqlite3 as sql
from copy import deepcopy


def h(fname):
    return hashlib.sha1(fname).digest()


def toUni(hash_digest):
    return u''.join([u'{:02x}'.format(ord(c)) for c in hash_digest])


class Storage(object):

    def __init__(self, table_path):
        # conn = sql.connect( ''.join([D['fileDir'],'indexDB_',str(os.getpid()),'.db']) )
        conn = sql.connect(table_path)

        c = conn.cursor()

        c.execute('''DROP TABLE IF EXISTS storage;''')

        c.execute('''CREATE TABLE storage (
                id INT PRIMARY KEY,
                hash TEXT NOT NULL,
                version TEXT NOT NULL,
                file BLOB NOT NULL
            );''')

        conn.commit()

        self.db = conn

    # hash of file, server leading the write, prev_version, file blob
    def storeFile(self, hash_digest, writer, prev_version, file):
        if prev_version is None:
            version = {writer: 1}
        elif writer in prev_version:
            version = deepcopy(prev_version)
            version[writer] += 1
        else:
            version = deepcopy(prev_version)
            version[writer] = 1

        c = self.db.cursor()

        uHash = toUni(hash_digest)

        # print("Storing file: ", uHash, version, file)

        c.execute('''INSERT INTO storage (hash, version, file) VALUES (?,?,?);''',
                  (uHash, sql.Binary('%s' % version), sql.Binary(file)))

        self.db.commit()

    # returns a list of sorted clock,value pairs for each matching
    # row in the database
    def getFile(self, hash_digest):
        c = self.db.cursor()
        uHash = toUni(hash_digest)
        c.execute('''SELECT * FROM storage WHERE hash=?;''', (uHash,))
        rows = c.fetchall()

        return self.sortData([[eval('%s' % (r[2])), '%s' % (r[3])] for r in rows]) if rows is not None else None

    # remove all instances of a given hash from the db
    def remFile(self, hash_digest):
        c = self.db.cursor()
        uHash = toUni(hash_digest)
        c.execute('''DELETE FROM storage WHERE hash=?;''', (uHash,))
        self.db.commit()

    # returns 1 if val2 -> val1
    # otherwise 0
    def compare_clocks(self, val1, val2):
        vec1 = deepcopy(val1[0])  # duplicate dictionaries
        vec2 = deepcopy(val2[0])
        # make sure both dictionaries have same key set
        for key in val1[0].keys():
            if key not in val2[0]:
                vec2[key] = 0;

        for key in val2[0].keys():
            if key not in val1[0]:
                vec2[key] = 0;

            # convert to tuple list for sorting
        vec1 = vec1.items()
        vec2 = vec2.items()
        # sort key/val pairs by server name
        vec1.sort(key=(lambda x: x[0]))
        vec2.sort(key=(lambda x: x[0]))

        isGT = True  # all vals are gte to the second clock
        isStrict = False  # at least one val is strictly greater

        for vc1, vc2 in zip(vec1, vec2):
            if vc1 < vc2:
                isGT = False
            if vc1 > vc2:
                isStrict = True

        return 1 if isGT and isStrict else 0

    # returns the ordered pair such that if val1->val2 then (val2, val1)
    # else if val2->val1 then (val1, val2) else (val1, val2)
    def compare_and_swap(self, val1, val2):
        return [val1, val2] if self.compare_clocks(val1, val2) else (
            [val2, val1] if self.compare_clocks(val2, val1) else [val1, val2]
        )

    # bubble sort for vector clocks
    # Sorts list of (clock, value) pairs in descending order of occurance
    def sortData(self, values):
        for i in range(len(values))[::-1]:
            for j in range(i):
                values[j], values[j + 1] = self.compare_and_swap(values[j], values[j + 1])

        return values

    # merge together the vector clocks of two concurrent versions of data
    # such that each clock value is the max of the prev two
    # this will rejoin the branches of the version history
    def mergeClocks(self, clock1, clock2):
        comb_clock = deepcopy(clock1)
        for serv, clock in clock2.items():
            if serv in comb_clock:
                comb_clock[serv] = max(comb_clock[serv], clock)
            else:
                comb_clock[serv] = clock
        return comb_clock


if __name__ == '__main__':
    db = Storage(':memory:')

    db.storeFile(h('testFile'), 's1', None, 'This is a test file 0')

    # concurrent write operations by s1 and s2
    prev = db.getFile(h('testFile'))[0][0]
    db.storeFile(h('testFile'), 's1', prev, 'This is a test file 1')
    db.storeFile(h('testFile'), 's2', prev, 'This is a test file 2')

    results = db.getFile(h('testFile'))
    prev = db.mergeClocks(results[0][0], results[1][0])

    # store version that was reconciled by client
    db.storeFile(h('testFile'), 's3', prev, 'This is a test file 3')

    prev = db.getFile(h('testFile'))[0][0]

    db.storeFile(h('testFile'), 's1', prev, 'This is a test file 4')

    prev = db.getFile(h('testFile'))[0][0]

    db.storeFile(h('testFile'), 's2', prev, 'This is a test file 5')

    prev = db.getFile(h('testFile'))[0][0]

    db.storeFile(h('testFile'), 's2', prev, 'This is a test file 6')

    prev = db.getFile(h('testFile'))[0][0]

    db.storeFile(h('testFile'), 's1', prev, 'This is a test file 7')

    prev = db.getFile(h('testFile'))[0][0]

    db.storeFile(h('testFile'), 's4', prev, 'This is a test file 8')

    prev = db.getFile(h('testFile'))[0][0]

    db.storeFile(h('testFile'), 's4', prev, 'This is a test file 9')

    print(db.getFile(h('testFile')))

    db.remFile(h('testFile'))

    print(db.getFile(h('testFile')))
