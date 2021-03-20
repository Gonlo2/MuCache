#!/usr/bin/env python3
import logging
import os
import os.path
import stat

from exiftool import ExifTool

from .types import ST_KEYS, Entry

IN_MOVED_FROM    = 0x00000040
IN_MOVED_TO      = 0x00000080
IN_CLOSE_WRITE   = 0x00000008
IN_CREATE        = 0x00000100
IN_DELETE        = 0x00000200


logger = logging.getLogger(__name__)


def check_flag(mask, *options):
    return any(map(lambda x: (x & mask) == x, options))


class FileBuilder:
    def __init__(self, path, storage, proxy, power_manager):
        self._path = path
        self._storage = storage
        self._proxy = proxy
        self._power_manager = power_manager
        self._next_id = self._storage.get_largest_id() + 1

    def inotify(self, e):
        if check_flag(e.mask, IN_MOVED_FROM, IN_DELETE):
            p = os.path.join('/', e.path, e.name)
            self._del_path(p)
        if check_flag(e.mask, IN_CLOSE_WRITE, IN_MOVED_TO, IN_CREATE):
            p = os.path.join('/', e.path)
            parent_id = self._storage.get_id(p)
            if parent_id is not None:
                relpath = os.path.join(self._path, e.path, e.name)
                self._setup_and_add_path(parent_id, relpath)
        if self._proxy is not None:
            self._proxy.notify(e)

    def rebuild(self):
        logger.debug("Indexing files")
        self._storage.purge()
        self._next_id = 0
        self._setup_and_add_path(-1, self._path)

    def _setup_and_add_path(self, parent_id, path):
        with ExifTool() as exif_tool:
            try:
                self._power_manager.acquire()
                self._add_path(parent_id, path, exif_tool)
            finally:
                self._power_manager.release()

    def _add_path(self, parent_id, path, exif_tool):
        to_check = [(parent_id, os.path.abspath(path), None)]

        entries = []
        while to_check:
            parent_id, path, fstat = to_check.pop()
            e = self._create(self._next_id, parent_id, path, exif_tool, fstat=fstat)
            entries.append(e)
            if stat.S_ISDIR(e.st_mode):
                with os.scandir(path) as it:
                    for entry in reversed(sorted(it, key=lambda e: e.name)):
                        to_check.append((self._next_id, entry.path, entry.stat()))
            self._next_id += 1

        self._storage.replace_entries(entries)

    def _del_path(self, relpath):
        id = self._storage.get_id(relpath)
        if id is not None:
            self._del_id(id)

    def _del_id(self, id):
        ids_to_remove = [id]
        while ids_to_remove:
            id = ids_to_remove.pop()
            children_ids = self._storage.get_children_ids(id)
            ids_to_remove.extend(children_ids or [])
            self._storage.remove_entry(id)

    def _create(self, id, parent_id, path, exif_tool, fstat=None):
        logger.debug(f"Creating entry of path '{path}' with id {id}")
        data = {}
        data['id'] = id
        data['parent_id'] = parent_id
        relpath = os.path.relpath(path, start=self._path)
        relpath = '/' if relpath == '.' else f"/{relpath}"
        data['path'] = relpath
        data['name'] = os.path.basename(path)

        if fstat is None:
            fstat = os.stat(path)
        if stat.S_ISREG(fstat.st_mode):
            data['duration'] = exif_tool.get_tag('Duration', path)
        for key in ST_KEYS:
            data[key] = getattr(fstat, key)
        data['st_ino'] = id

        return Entry(**data)
