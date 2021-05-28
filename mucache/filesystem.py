#!/usr/bin/env python3
import logging
import os
import os.path
import time
from queue import Queue
from threading import Lock, Thread

from .file import File
from .types import State

logger = logging.getLogger(__name__)


class Filesystem:
    def __init__(self, *, src_path, dst_path, storage, power_manager,
                 cleaner, prefetch_sec, prefetch_bytes):
        self._src_path = src_path
        self._dst_path = dst_path
        self._storage = storage
        self._power_manager = power_manager
        self._cleaner = cleaner
        self._prefetch_sec = prefetch_sec
        self._prefetch_bytes = prefetch_bytes
        self._lock = Lock()
        self._files_by_id = {}
        self._loop_queue = Queue()
        self._thread = None

    def get_attr(self, path):
        return self._storage.get_attr(path)

    def read_dir(self, path):
        id = self._storage.get_id(path)
        if id is None:
            return None
        names = self._storage.get_children_names(id)
        if names is None:
            return None
        names.sort()
        return names

    def open(self, path):
        with self._lock:
            f, fid = self._touch_file(path)
            if f is None:
                return None
            f.open()
            return fid

    def _touch_file(self, path):
        fid, state, size = self._storage.get_id_state_size(path)
        if fid is None:
            return (None, None)
        f = self._get_file(fid, path, state, size)
        self._storage.set_last_access_ts(fid, int(time.time()))
        return (f, fid)

    def _get_file(self, id, path, state, size):
        f = self._files_by_id.get(id)
        if f is None:
            f = File(
                os.path.join(self._src_path, path[1:]),
                os.path.join(self._dst_path, str(id)),
                state,
                size,
                self._power_manager
            )
            self._files_by_id[id] = f
            if state == State.CACHED and size < self._prefetch_bytes:
                next_path, next_state = self._storage.get_next_file_path_state(path)
                if next_state == State.NO_CACHED:
                    self._cache_next_files(next_path)
        return f

    def read(self, path, fh, length, offset):
        with self._lock:
            f = self._files_by_id.get(fh)
            if f is None:
                return None
        start_caching, data = f.read(length, offset)
        if start_caching:
            with self._lock:
                self._cache_next_files(path)
        return data

    def _cache_next_files(self, path):
        to_cache = self._storage.get_next_files_to_cache(
            path,
            self._prefetch_sec,
            self._prefetch_bytes,
        )
        ts = int(time.time())
        for i, (fid, path, size) in enumerate(to_cache):
            logger.debug(f"To precache the file '{path}' with id {fid}")
            self._storage.set_last_access_ts(fid, ts-i)
            self._storage.set_state(fid, State.NO_CACHED, State.CACHING)
            self._loop_queue.put(path)

    def close(self, fh):
        with self._lock:
            f = self._files_by_id.get(fh)
            if f is None:
                return False
            self._close(f, fh)
            return True

    def _close(self, f, id):
        if f.close():
            self._files_by_id.pop(id)

    def start(self):
        self._thread = Thread(target=self._loop)
        self._thread.start()

    def _loop(self):
        while True:
            path = self._loop_queue.get()
            if path is None:
                break
            with self._lock:
                f, fid = self._touch_file(path)
            if f is not None:
                f.open()
                self._cleaner.to_add(f.size())
                logger.debug(f"Caching the file '{path}' with id {fid}")
                while f.cache_next_chunk():
                    pass
                logger.debug(f"Cached the file '{path}' with id {fid}")
                with self._lock:
                    self._storage.set_state(fid, State.CACHING, State.CACHED)
                    self._close(f, fid)

    def stop(self):
        self._loop_queue.put(None)
        self._thread.join()
        self._thread = None
