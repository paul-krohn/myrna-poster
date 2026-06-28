from argparse import ArgumentParser, Namespace
import hashlib
import logging
import os
import re
import threading
import time

import requests
from retrying import retry
from statsd import StatsClient
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

parser = ArgumentParser()
parser.add_argument("input_path", help="input directory")
parser.add_argument('--api', default=os.getenv("POSTER_API"), required=True)
parser.add_argument('--camera', help="override the directory name with this camera name")
parser.add_argument('--log-level', default="WARN")
parser.add_argument('--statsd-host', default='localhost')
parser.add_argument('--statsd-port', default=8125)

args = parser.parse_args()

camera_name = args.camera if args.camera else os.path.basename(args.input_path.strip("/"))

stats = StatsClient(host=args.statsd_host, port=args.statsd_port, prefix="poster")


def _set_up_logging():

    level = logging.getLevelName(args.log_level.upper())
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')

    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    return logger

logger = _set_up_logging()

class ChecksumException(Exception):
    pass

def raise_checksum_exception():
    logger.info(f"checksum exception")
    stats.incr(f"checksum.exception#camera={camera_name}")
    raise ChecksumException


class FileStoreException(Exception):
    pass

def raise_file_store_exception():
    logger.info(f"file storage exception")
    stats.incr(f"file.storage.exception#camera={camera_name}")
    raise FileStoreException

class DbUpdateException(Exception):
    pass

def raise_db_update_exception():
    logger.info(f"db update exception")
    stats.incr(f"db.update.exception#camera={camera_name}")
    raise DbUpdateException

@stats.timer(f"segment_checksum#camera={camera_name}")
def segment_checksum(filename):
    logger.debug(f"calculating checksum for {filename}")
    BUF_SIZE = 1048576
    segment_sha1 = hashlib.sha1()
    with open(filename, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            segment_sha1.update(data)
    logger.debug(f"checksum for {filename}: {segment_sha1.hexdigest()}")
    return segment_sha1.hexdigest()


class SegmentSender:
    def __init__(self, args: Namespace):
        self.api_url = args.api
        self.api_session = self._api_session()
        self.camera = args.camera if args.camera else os.path.basename(args.input_path.strip("/"))
        self.counter = 0

    def _api_session(self):
        session = requests.Session()
        token_url = f"{self.api_url}login/"
        logger.debug(f"token url: {token_url}")
        r = session.get(token_url)
        logger.debug(r.content)
        token = r.json()["token"]
        session.headers.update({'X-CSRFToken': token})
        return session

    @stats.timer(f"send#camera={camera_name}")
    @retry(wait_random_min=1000, wait_random_max=2000, stop_max_attempt_number=5,
           retry_on_exception=lambda e: not isinstance(e, FileNotFoundError) and (logger.warning(f"retrying after: {e!r}") or True))
    def send(self, filename):
        logger.debug(f"sending {filename}")
        segment_sha1 = segment_checksum(filename)
        api_url = f"{self.api_url}segment/upload/{self.camera}/"
        logger.debug(f"sending {filename} to {api_url}")
        response = self.api_session.post(
            api_url,
            files={'segment': open(filename, 'rb')},
            data={'sha1': segment_sha1}
        )
        result = response.json()
        # ideally, the response looks like:
        # {"checksum": "pass", "duration": 3.999178, "start_time": 313.884178, "db_stored": true}
        if not result["checksum"]:
            raise_checksum_exception()
        elif not result["duration"] > 0.0:
            raise_file_store_exception()
        elif not result["db_stored"]:
            raise_db_update_exception()
        else:
            logger.info(f"sent {filename} to {api_url} with response: {response.content}")
            self.counter += 1
            if self.counter % 8 == 0:
                logger.info(f"sent {self.counter} segments since startup")
            result = response.json()
            stats.incr(f"segment_sent#camera={camera_name}")
            stats.gauge(f"remote_segment_duration#camera={camera_name}", result["duration"])

def _wait_until_stable(path, interval=0.3, required=2):
    """Block until the file size is unchanged for `required` consecutive checks."""
    prev, streak = -1, 0
    while streak < required:
        try:
            cur = os.path.getsize(path)
        except FileNotFoundError:
            return
        streak = streak + 1 if cur == prev and cur > 0 else 0
        prev = cur
        time.sleep(interval)


class NewSegmentHandler(FileSystemEventHandler):
    _upload_sem = threading.Semaphore(4)

    def on_any_event(self, event):
        logger.debug(f"file {event.src_path} {event.event_type}")

    def on_moved(self, event: FileSystemEvent) -> None:
        dest = event.dest_path
        if not re.search(r'\.ts$', dest):
            return
        stats.incr(f"file_moved#camera={camera_name}")
        def _worker():
            t0 = time.time()
            _wait_until_stable(dest)
            t1 = time.time()
            stats.gauge(f"wait_stable#camera={camera_name}", t1 - t0)
            with self._upload_sem:
                t2 = time.time()
                stats.gauge(f"sem_wait#camera={camera_name}", t2 - t1)
                sender = SegmentSender(args)
                t3 = time.time()
                stats.gauge(f"session_init#camera={camera_name}", t3 - t2)
                try:
                    sender.send(dest)
                    logger.info(f"timing send={time.time()-t3:.2f}s {dest}")
                except FileNotFoundError:
                    logger.warning(f"file disappeared before send: {dest}")
                    stats.incr(f"file.disappeared#camera={camera_name}")
                except Exception as e:
                    logger.error(f"failed to send {dest}: {e!r}")
                    stats.incr(f"send.failed#camera={camera_name}")
        threading.Thread(target=_worker, daemon=True).start()

event_handler = NewSegmentHandler()
observer = Observer()
observer.schedule(event_handler, args.input_path, recursive=True)
observer.start()
try:
    while True:
        time.sleep(1)
finally:
    observer.stop()
    observer.join()
