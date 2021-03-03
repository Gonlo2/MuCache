#!/usr/bin/env python3

class ReadStrategy:
    def read(self, length, offset):
        raise NotImplementedError

    def cache_next_chunk(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class DirectReadStrategy(ReadStrategy):
    def __init__(self, fd):
        self._fd = fd

    def read(self, length, offset):
        self._fd.seek(offset)
        return self._fd.read(length)

    def cache_next_chunk(self):
        return False

    def close(self):
        self._fd.close()
        self._fd = None
        return True


class CacheReadStrategy(ReadStrategy):
    def __init__(self, src_fd, dst_fd, chunks):
        self._src_fd = src_fd
        self._dst_fd = dst_fd
        self._chunks = chunks

    def read(self, length, offset):
        self._chunks.ensure_in_cache(self._src_fd, self._dst_fd, length, offset)
        self._dst_fd.seek(offset)
        return self._dst_fd.read(length)

    def cache_next_chunk(self):
        return self._chunks.cache_next_chunk(
            self._src_fd,
            self._dst_fd
        )

    def close(self):
        self._src_fd.close()
        self._src_fd = None
        self._dst_fd.close()
        self._dst_fd = None
        return True
