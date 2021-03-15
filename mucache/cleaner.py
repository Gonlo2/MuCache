#!/usr/bin/env python3
import logging
import os
import os.path
from queue import Queue
from threading import Thread

from .types import State

logger = logging.getLogger(__name__)


class Cleaner:
    def __init__(self, path, storage, limit_in_bytes, retention_factor=0.6, expire_in_sec=60*60*8):
        self._path = path
        self._storage = storage
        self._limit_in_bytes = limit_in_bytes
        self._retention_factor = retention_factor
        self._expire_in_sec = expire_in_sec

        self._thread = None
        self._loop_queue = Queue()

    def to_add(self, n_bytes):
        self._loop_queue.put(n_bytes)

    def start(self):
        self._thread = Thread(target=self._loop)
        self._thread.start()

    def _loop(self):
        limit_in_bytes = self._limit_in_bytes * self._retention_factor
        used_bytes = self._cleanup(limit_in_bytes)

        while True:
            n_bytes = self._loop_queue.get()
            if n_bytes is None:
                break
            used_bytes += n_bytes
            if used_bytes > self._limit_in_bytes:
                used_bytes = self._cleanup(limit_in_bytes)

    def _cleanup(self, limit_in_bytes):
        self._uncache_removed_cache_files()
        used_bytes = self._uncache_old_files(limit_in_bytes)
        self._remove_uncached_cache_files()
        return used_bytes

    def _uncache_removed_cache_files(self):
        for id in self._storage.get_cached_ids():
            path = os.path.join(self._path, str(id))
            if not os.path.exists(path):
                logger.warning(f"Unmarking the removed cache file with id {id}")
                self._storage.set_state(id, State.CACHED, State.NO_CACHED)

    def _uncache_old_files(self, limit_in_bytes):
        used_bytes = self._storage.get_cached_bytes()
        while used_bytes > limit_in_bytes:
            has_more, rows = self._storage.get_oldest_cached_files()
            for id, n_bytes in rows:
                logger.debug(f"Unmarking the old cache file with id {id}")
                self._storage.set_state(id, State.CACHED, State.NO_CACHED)
                used_bytes -= n_bytes
                if used_bytes <= limit_in_bytes:
                    break
            if not has_more:
                break
        return used_bytes

    def _remove_uncached_cache_files(self):
        with os.scandir(self._path) as it:
            for entry in it:
                if entry.is_file() and not self._is_valid_cache_file(entry):
                    logger.debug(f"Removing the cache file '{entry.path}'")
                    try:
                        os.remove(entry.path)
                    except:
                        logger.exception(f"Error removing the cached file {id}")

    def _is_valid_cache_file(self, entry):
        try:
            id = int(entry.name)
        except ValueError:
            logger.warning("The cache file '{entry.name}' isn't a number")
            return False
        state, size = self._storage.get_state_size(id)
        if state in (None, State.NO_CACHED):
            return False
        if state == State.CACHED and size != entry.stat().st_size:
            self._storage.set_state(id, State.CACHED, State.NO_CACHED)
            return False
        return True

    def stop(self):
        self._loop_queue.put(None)
        self._thread.join()
        self._thread = None
