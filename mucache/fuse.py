#!/usr/bin/env python3
import errno
import logging

from fuse import FuseOSError, Operations

logger = logging.getLogger(__name__)


class FuseWrapper(Operations):
    def __init__(self, file_manager):
        self._file_manager = file_manager

    def getattr(self, path, fh=None):
        logger.debug('Obtaining the attributes of "%s"', path)
        res = self._file_manager.get_attr(path)
        if res is None:
            raise FuseOSError(errno.ENOENT)
        return res

    def readdir(self, path, fh):
        logger.debug('Reading the dir "%s"', path)
        names = self._file_manager.read_dir(path)
        if names is None:
            raise FuseOSError(errno.ENOENT)
        return ['.', '..'] + names

    def open(self, path, flags):
        logger.debug('Opening the file "%s"', path)
        fh = self._file_manager.open(path)
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
        data = self._file_manager.read(path, fh, length, offset)
        if data is None:
            raise FuseOSError(errno.ENOENT)
        return data

    def release(self, path, fh):
        logger.debug('Closing the file "%s" with fh %d', path, fh)
        if not self._file_manager.close(fh):
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
