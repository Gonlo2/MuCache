#!/usr/bin/env python3

class FileChunks:
    def __init__(self, size, chunk_size_bits=18):
        self._num_chunks = ((size-1) >> chunk_size_bits) + 1
        self._chunk_size_bits = chunk_size_bits
        self._next_chunk = 0
        self._cached_chunks = set()

    def ensure_in_cache(self, src_fd, dst_fd, length, offset):
        a = offset >> self._chunk_size_bits
        b = (offset+length) >> self._chunk_size_bits
        while a <= b:
            if a >= self._next_chunk and a not in self._cached_chunks:
                self._copy_chunk(src_fd, dst_fd, a)
                if a == self._next_chunk:
                    self._next_chunk += 1
                else:
                    self._cached_chunks.add(a)
            a += 1

    def cache_next_chunk(self, src_fd, dst_fd):
        if self._next_chunk < self._num_chunks:
            self._copy_chunk(src_fd, dst_fd, self._next_chunk)
            self._cached_chunks.discard(self._next_chunk)
            self._next_chunk += 1
        return self._next_chunk < self._num_chunks

    def _copy_chunk(self, src_fd, dst_fd, i):
        src_fd.seek(i << self._chunk_size_bits)
        dst_fd.seek(i << self._chunk_size_bits)
        size = 1 << self._chunk_size_bits
        while size > 0:
            chunk = src_fd.read(size)
            if not chunk:
                break
            dst_fd.write(chunk)
            size -= len(chunk)
