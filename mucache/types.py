#!/usr/bin/env python3
import dataclasses
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional


class State(IntEnum):
    NO_CACHED = 0
    CACHING = 1
    CACHED = 2


@dataclass
class Entry:
    id: int
    parent_id: int
    path: str
    name: str
    state: State = State.NO_CACHED
    last_access_ts: Optional[int] = None
    duration: Optional[int] = None

    st_mode: Optional[int] = None
    st_ino: Optional[int] = None
    st_dev: Optional[int] = None
    st_nlink: Optional[int] = None
    st_uid: Optional[int] = None
    st_gid: Optional[int] = None
    st_size: Optional[int] = None
    st_atime: Optional[int] = None
    st_ctime: Optional[int] = None
    st_mtime: Optional[int] = None


ST_KEYS = [f.name for f in dataclasses.fields(Entry) if f.name.startswith('st_')]
