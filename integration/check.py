"""Own the test JVM lifecycle so CI also verifies backfill across a real restart."""
import subprocess
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import indexes
import items
import lifecycle
import lock

ROOT = Path(__file__).resolve().parents[1]


class Server:
    def __init__(self):
        self.process = None

    def start(self):
        self.process = subprocess.Popen([str(ROOT / "build/install/tikronos/bin/tikronos")], cwd=ROOT)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"Test JVM exited with {self.process.returncode}")
            try:
                with urlopen("http://127.0.0.1:8000/healthz", timeout=1) as response:
                    if response.status == 200:
                        return
            except (URLError, TimeoutError):
                pass
            time.sleep(.1)
        raise AssertionError("Test JVM did not become healthy")

    def stop(self):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
                raise AssertionError("Test JVM did not shut down cleanly") from None
            finally:
                self.process = None

    def restart(self):
        previous = self.process.pid
        self.stop()
        self.start()
        assert self.process.pid != previous


def main():
    server = Server()
    try:
        server.start()
        lock.main()
        items.exercise()
        indexes.exercise(restart=server.restart)
        lifecycle.exercise()
    finally:
        server.stop()
    print("PASS HTTP checks and JVM restart during CREATING index backfill", flush=True)


if __name__ == "__main__":
    main()
