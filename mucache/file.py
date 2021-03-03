#!/usr/bin/env python3
import time
from threading import Lock

from .file_chunks import FileChunks
from .read_strategy import CacheReadStrategy, DirectReadStrategy, ReadStrategy
from .types import State

MB = 1024 * 1024


class BytesReaden:
    def __init__(self):
        self._expiration_ts: int = 0
        self._value: int = 0

    def incr(self, v):
        t = int(time.time())
        if self._expiration_ts < t:
            self._expiration_ts = t + 300
        self._value += v
        return self._value


class File(ReadStrategy):
    def __init__(self, src_path, dst_path, state, size, power_manager):
        self._src_path = src_path
        self._dst_path = dst_path
        self._state = state
        self._size = size
        self._power_manager = power_manager

        self._passthrow_limit = max(16 * MB, min(0.15 * size, 64 * MB))
        self._lock = Lock()
        self._rc = 0
        self._chunks = None
        self._bytes_readen = BytesReaden()
        self._strategy = None

        if self._state == State.CACHING:
            self._setup_chunks()

    def _setup_chunks(self):
        self._chunks = FileChunks(self._size)

    def size(self):
        return self._size

    def open(self):
        with self._lock:
            if self._rc == 0:
                self._open()
            self._rc += 1

    def _open(self):
        ctor_by_state = {
            State.NO_CACHED: self._open_no_cached,
            State.CACHING: self._open_caching,
            State.CACHED: self._open_cached,
        }
        self._strategy = ctor_by_state[self._state]()

    def _open_no_cached(self):
        self._power_manager.acquire()

        return DirectReadStrategy(
            open(self._src_path, 'rb'),
        )

    def _open_caching(self):
        self._power_manager.acquire()

        with open(self._dst_path, 'wb') as f:
            f.seek(self._size)
            f.write(b'\0')
            f.truncate(self._size)

        return CacheReadStrategy(
            open(self._src_path, 'rb'),
            open(self._dst_path, 'rb+'),
            self._chunks,
        )

    def _open_cached(self):
        return DirectReadStrategy(
            open(self._dst_path, 'rb'),
        )

    def read(self, length, offset):
        with self._lock:
            start_caching = False
            if self._state == State.NO_CACHED:
                bytes_readen = self._bytes_readen.incr(length)
                if bytes_readen >= self._passthrow_limit:
                    self._change_state_to_caching()
                    start_caching = True
            data = self._strategy.read(length, offset)
            return (start_caching, data)

    def cache_next_chunk(self):
        with self._lock:
            if self._state == State.NO_CACHED:
                self._change_state_to_caching()
            if self._state == State.CACHING:
                if not self._strategy.cache_next_chunk():
                    self._chunks = None
                    self._close()
                    self._state = State.CACHED
                    self._open()
            if self._state == State.CACHED:
                return False
            return True

    def _change_state_to_caching(self):
        self._setup_chunks()
        self._close()
        self._state = State.CACHING
        self._open()

    def close(self):
        with self._lock:
            self._rc -= 1
            if self._rc == 0:
                self._close()
            return self._rc == 0

    def _close(self):
        if self._state in (State.NO_CACHED, State.CACHING):
            self._power_manager.release()
        self._strategy.close()
        self._strategy = None
