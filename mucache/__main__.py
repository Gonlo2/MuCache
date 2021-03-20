import logging
from argparse import (ArgumentDefaultsHelpFormatter, ArgumentParser,
                      ArgumentTypeError)

from fuse import FUSE
from power_manager.client import Client as PowerManagerClient
from reinotify.proxy import Proxy as ReinotifyProxy
from reinotify.server import Server as ReinotifyServer

from .cleaner import Cleaner
from .file_builder import FileBuilder
from .filesystem import Filesystem
from .fuse import FuseWrapper
from .storage import SqliteWrapper, Storage
from .types import State

logger = logging.getLogger(__name__)


GIB = 1024 * 1024 * 1024


def create_arg_parser():
    p = ArgumentParser(
        description="Multimedia cache to allow expose some remote files as local ones using fuse.",
        prog="python -m mucache",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    p.add_argument('src_path', help='src path')
    p.add_argument('fuse_path', help='dst path where mount fuse fs')
    p.add_argument('cache_path', help='cache path')
    p.add_argument('--pm-address', default='http://127.0.0.1:9353',
                   help='power manager address')
    p.add_argument('--pm-token-id', default='mucache',
                   help='the power manager token id')
    p.add_argument('--reinotify', default=Address('127.0.0.1', 4444),
                   type=type_address, help='reinotify listening host and port')
    p.add_argument('--reinotify-forward', default=None,
                   type=type_address, help='reinotify forward host and port')
    p.add_argument('--db-path', default='db.sqlite',
                   help='path of the sqlite database')
    p.add_argument('--cache-limit', default=180, type=int,
                   help='cache size limit in gibibytes')
    p.add_argument('--prefetch-min', default=180, type=int,
                   help='maximum number of minutes to prefetch')
    p.add_argument('--prefetch-gib', default=10, type=int,
                   help='maximum number of gibibytes to prefetch')
    p.add_argument('--rebuild', action='store_true',
                   help='purge the DB and index the files')
    p.add_argument('--log-level', default='INFO',
                   choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
                   help='logger level')
    return p


def type_address(x):
    host_and_port = x.split(':', 1)
    if len(host_and_port) != 2:
        raise ArgumentTypeError("The expected format is <host>:<port>")

    try:
        return Address(host_and_port[0], int(host_and_port[1]))
    except ValueError:
        raise ArgumentTypeError("The port must be a valid number")


class Address(tuple):
    def __new__(self, host, port):
        return tuple.__new__(Address, (host, port))

    def __repr__(self):
        return f"{self[0]}:{self[1]}"


def main():
    parser = create_arg_parser()
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    logger.debug("Starting DB")
    db = SqliteWrapper(args.db_path)

    logger.debug("Starting storage")
    storage = Storage(db)
    storage.setup()

    logger.debug("Starting power manager")
    pm = PowerManagerClient(args.pm_address, token_id=args.pm_token_id)
    pm.start()

    if args.reinotify_forward is not None:
        logger.debug("Starting remote watcher proxy")
        reinotify_proxy = ReinotifyProxy(args.reinotify_forward)
    else:
        reinotify_proxy = None

    logger.debug("Starting file builder")
    file_builder = FileBuilder(args.src_path, storage, reinotify_proxy, pm)

    if args.rebuild:
        file_builder.rebuild()
    else:
        storage.set_states(State.CACHING, State.NO_CACHED)

    logger.debug("Starting remote watcher server")
    reinotify_server = ReinotifyServer(args.reinotify, file_builder.inotify)
    reinotify_server.start()

    logger.debug("Starting cleaner")
    cleaner = Cleaner(args.cache_path, storage, args.cache_limit * GIB)
    cleaner.start()

    logger.debug("Starting file manager")
    fs = Filesystem(src_path=args.src_path, dst_path=args.cache_path,
                    storage=storage, power_manager=pm, cleaner=cleaner,
                    prefetch_sec=args.prefetch_min * 60,
                    prefetch_bytes=args.prefetch_gib * GIB)
    fs.start()

    try:
        logger.info("Starting FUSE")
        FUSE(FuseWrapper(fs), args.fuse_path, nothreads=False,
             foreground=True, allow_other=True, ro=True)
    finally:
        fs.stop()
        cleaner.stop()


if __name__ == '__main__':
    main()
