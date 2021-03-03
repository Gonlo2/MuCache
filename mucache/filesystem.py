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
    def __init__(self, src_path, dst_path, storage, power_manager, cleaner):
        self._src_path = src_path
        self._dst_path = dst_path
        self._storage = storage
        self._power_manager = power_manager
        self._cleaner = cleaner
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
            id, state, size = self._storage.get_id_state_size(path)
            if id is None:
                return None
            self._open(id, path, state, size)
            self._storage.set_last_access_ts(id, int(time.time()))
            return id

    def _open(self, id, path, state, size):
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
            if state == State.CACHED:
                next_path, next_state = self._storage.get_next_file_path_state(path)
                if next_state == State.NO_CACHED:
                    self._cache_next_files(next_path)
        f.open()
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
            240 * 60 # TODO
        )
        ts = int(time.time())
        for i, (id, path, size) in enumerate(to_cache):
            logger.debug(f"To precache the file '{path}' with id {id}")
            self._storage.set_last_access_ts(id, ts-i)
            self._storage.set_state(id, State.NO_CACHED, State.CACHING)
            f = self._open(id, path, State.CACHING, size)
            self._loop_queue.put((f, id))

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
            w = self._loop_queue.get()
            if w is None:
                break
            f, id = w
            self._cleaner.to_add(f.size())
            logger.debug(f"Caching the file with id {id}")
            while f.cache_next_chunk():
                pass
            logger.debug(f"Cached the file with id {id}")
            with self._lock:
                self._storage.set_state(id, State.CACHING, State.CACHED)
                self._close(f, id)

    def stop(self):
        self._loop_queue.put(None)
        self._thread.join()
        self._thread = None
