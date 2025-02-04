from argparse import ArgumentParser
import re
import time

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

parser = ArgumentParser()
parser.add_argument("input_path", help="input directory")
args = parser.parse_args()

class MyEventHandler(FileSystemEventHandler):
    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.event_type == "closed" and re.search(r'\.ts$', event.src_path):
            print(f"file {event.src_path} {event.event_type}")
        else:
            print(f"ignoring event type {event.event_type}")


event_handler = MyEventHandler()
observer = Observer()
observer.schedule(event_handler, args.input_path, recursive=True)
observer.start()
try:
    while True:
        time.sleep(1)
finally:
    observer.stop()
    observer.join()