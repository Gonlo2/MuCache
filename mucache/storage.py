#!/usr/bin/env python3
import dataclasses
import sqlite3
import stat
from threading import Lock

from .types import ST_KEYS, Entry, State


class SqliteWrapper:
    def __init__(self, path):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._lock = Lock()

    def read_one(self, query, args=None):
        with self._lock:
            c = self._db.execute(query) \
                    if args is None \
                    else self._db.execute(query, args)
            r = c.fetchone()
            c.close()
            return r

    def read_all(self, query, args=None):
        with self._lock:
            c = self._db.execute(query) \
                    if args is None \
                    else self._db.execute(query, args)
            r = c.fetchall()
            c.close()
            return r

    def write(self, query, args=None):
        with self._lock:
            with self._db:
                if args is None:
                    self._db.execute(query)
                else:
                    self._db.execute(query, args)

    def write_many(self, query, seq_of_parameters):
        with self._lock:
            with self._db:
                self._db.executemany(query, seq_of_parameters)

    def close(self):
        with self._lock:
            self._db.close()


class Storage:
    def __init__(self, db):
        self._db = db

    def setup(self):
        for q in self._get_create_tables():
            self._db.write(q)

    def _get_create_tables(self):
        yield '''CREATE TABLE IF NOT EXISTS filesystem (
            id INTEGER NOT NULL,
            parent_id INTEGER NOT NULL, -- The root have a id -1
            path TEXT NOT NULL,
            name TEXT NOT NULL,
            state INTEGER NOT NULL DEFAULT 0, -- Enum: 0 = no cached, 1 = caching, 2 = cached
            last_access_ts INTEGER,
            duration INTEGER, -- The duration of the video files, it is null in other case
            st_mode INTEGER,
            st_ino INTEGER,
            st_dev INTEGER,
            st_nlink INTEGER,
            st_uid INTEGER,
            st_gid INTEGER,
            st_size INTEGER,
            st_atime INTEGER,
            st_ctime INTEGER,
            st_mtime INTEGER,
            PRIMARY KEY (id)
        )'''
        yield 'CREATE INDEX IF NOT EXISTS parent_id ON filesystem (parent_id)'
        yield 'CREATE UNIQUE INDEX IF NOT EXISTS path ON filesystem (path)'
        yield 'CREATE INDEX IF NOT EXISTS last_access_ts ON filesystem (state, last_access_ts)'

    def replace_entries(self, entries):
        keys = list(sorted(f.name for f in dataclasses.fields(Entry)))
        values = [f':{k}' for k in keys]
        query = f"REPLACE INTO filesystem ({','.join(keys)}) values ({','.join(values)})"
        args = [dataclasses.asdict(e) for e in entries]
        self._db.write_many(query, args)

    def get_attr(self, path):
        query = (f"SELECT {','.join(ST_KEYS)} "
                 "FROM filesystem "
                 "WHERE path = ?")
        res = self._db.read_one(query, (path,))
        if res is None:
            return None
        return dict(zip(ST_KEYS, res))

    def get_id(self, path):
        query = "SELECT id FROM filesystem WHERE path = ?"
        res = self._db.read_one(query, (path,))
        if res is None:
            return None
        return res[0]

    def get_id_state_size(self, path):
        query = ("SELECT id, state, st_size "
                 "FROM filesystem "
                 "WHERE path = ?")
        res = self._db.read_one(query, (path,))
        if res is None:
            return (None, None, None)
        return (res[0], State(res[1]), res[2])

    def get_state_size(self, id):
        query = "SELECT state, st_size FROM filesystem WHERE id = ?"
        res = self._db.read_one(query, (id,))
        if res is None:
            return (None, None)
        return (res[0], res[1])

    def get_children_names(self, parent_id):
        query = "SELECT name FROM filesystem WHERE parent_id = ?"
        res = self._db.read_all(query, (parent_id,))
        if res is None:
            return None
        return [x for x, in res]

    def get_children_ids(self, parent_id):
        query = "SELECT id FROM filesystem WHERE parent_id = ?"
        res = self._db.read_all(query, (parent_id,))
        if res is None:
            return None
        return [x for x, in res]

    def get_next_files_to_cache(self, path, max_duration):
        query = ("SELECT id, path, state, duration, st_size "
                 "FROM filesystem "
                 "WHERE path >= ? "
                 "ORDER BY path "
                 "LIMIT 50")
        rows = self._db.read_all(query, (path,))

        res = []
        acc_duration = 0
        for id, path, state, duration, size in rows:
            if duration is not None:
                if state == State.NO_CACHED:
                    res.append((id, path, size))

                acc_duration += duration
                if acc_duration > max_duration:
                    break
        return res

    def get_next_file_path_state(self, path):
        query = ("SELECT path, state, st_mode "
                 "FROM filesystem "
                 "WHERE path > ? "
                 "ORDER BY path "
                 "LIMIT 8")
        res = self._db.read_all(query, (path,))
        for path, state, st_mode in (res or []):
            if stat.S_ISREG(st_mode):
                return (path, state)
        return (None, None)

    def set_state(self, id, old_state, new_state):
        query = "UPDATE filesystem SET state = ? WHERE id = ? and state = ?"
        self._db.write(query, (new_state, id, old_state))

    def set_states(self, old_state, new_state):
        query = "UPDATE filesystem SET state = ? WHERE state = ?"
        self._db.write(query, (new_state, old_state))

    def set_last_access_ts(self, id, ts):
        query = "UPDATE filesystem SET last_access_ts = ? WHERE id = ?"
        self._db.write(query, (ts, id))

    def get_cached_bytes(self):
        query = ("SELECT sum(st_size) "
                 "FROM filesystem "
                 "WHERE state = ?")
        return self._db.read_one(query, (State.CACHED,))[0] or 0

    def get_oldest_cached_files(self, limit=50):
        query = ("SELECT id, st_size "
                 "FROM filesystem "
                 "WHERE state = ? "
                 "ORDER BY last_access_ts "
                 "LIMIT ?")
        res = self._db.read_all(query, (State.CACHED, limit))
        return (len(res) == limit, res)

    def get_cached_ids(self):
        query = "SELECT id FROM filesystem WHERE state = ?"
        res = self._db.read_all(query, (State.CACHED,))
        return [x for x, in (res or [])]

    def remove_entry(self, id):
        query = "DELETE FROM filesystem WHERE id = ?"
        self._db.write(query, (id,))

    def get_largest_id(self):
        query = "SELECT max(id) FROM filesystem"
        return max(self._db.read_one(query)[0] or 0, 0)

    def purge(self):
        self._db.write('DELETE TABLE filesystem')
        self._db.write('VACUUM')
        self.setup()
