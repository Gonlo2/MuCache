#!/usr/bin/env python3
import errno
import logging

from fuse import FuseOSError, Operations

logger = logging.getLogger(__name__)


class FuseWrapper(Operations):
    def __init__(self, fs):
        self._fs = fs

    def getattr(self, path, fh=None):
        logger.debug('Obtaining the attributes of "%s"', path)
        res = self._fs.get_attr(path)
        if res is None:
            raise FuseOSError(errno.ENOENT)
        return res

    def readdir(self, path, fh):
        logger.debug('Reading the dir "%s"', path)
        names = self._fs.read_dir(path)
        if names is None:
            raise FuseOSError(errno.ENOENT)
        return ['.', '..'] + names

    def open(self, path, flags):
        logger.debug('Opening the file "%s"', path)
        fh = self._fs.open(path)
        if fh is None:
            raise FuseOSError(errno.ENOENT)
        return fh

    def read(self, path, length, offset, fh):
        logger.debug(
            'Reading the file "%s" (fh: %d, offset: %d, length: %d)',
            path,
            fh,
            offset,
            length
        )
        data = self._fs.read(path, fh, length, offset)
        if data is None:
            raise FuseOSError(errno.ENOENT)
        return data

    def release(self, path, fh):
        logger.debug('Closing the file "%s" with fh %d', path, fh)
        if not self._fs.close(fh):
            raise FuseOSError(errno.ENOENT)

    # Disable unused operations
    create = None
    write = None
    truncate = None
    flush = None
    fsync = None
    chmod = None
    chown = None
    mknod = None
    rmdir = None
    mkdir = None
    unlink = None
    symlink = None
    rename = None
    link = None
    statfs = None
    utimens = None
    access = None
    readlink = None
