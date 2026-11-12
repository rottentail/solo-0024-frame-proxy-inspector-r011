#!/usr/bin/env python3
"""Reproducible diagnosis for the "wedged identifier after a storage fault"
report against the Frame Proxy Inspector service.

Everything runs in an isolated sandbox (/tmp/frame-proxy-diag). The
repository under /workspace is never modified. Servers are the real
`python3 -m frame_proxy serve` processes; clients use http.client with
explicit timeouts so a hung request shows up as TIMEOUT instead of a
response.

Scenarios:
  A  (server A, pure, no shim): storage fault at FileKeyLock.__enter__
     (lock path is a directory -> open("a+b") raises IsADirectoryError).
     Shows: first request 500s, nothing is published, and AFTER the fault
     is cleared every later write for the same id hangs forever, retries
     never reach the 409 check, unrelated ids and all reads stay healthy,
     a second process on the same data root is NOT affected, and a
     restart changes the behaviour.
  B  (server C, shim): fault at the 3rd lock acquisition of an inspection
     workflow. Shows: the FIRST request itself hangs inside _mark_failed,
     the record stays published as "scanning" and can never advance.
  D  (server D, shim): fault at __exit__ (LOCK_UN). Shows: the client
     gets 500 yet the object was fully committed, and the id is wedged.
"""

import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

SRC = "/workspace/src"
DIAG = Path("/tmp/frame-proxy-diag")
SHIM = DIAG / "shim"
OUT = DIAG / "out"
DATA = DIAG / "data"
TRIGGER = DIAG / "fault-trigger"
AVI = DIAG / "sample.avi"

SERVERS = []


def banner(title: str) -> None:
    print("\n" + "=" * 72, flush=True)
    print(title, flush=True)
    print("=" * 72, flush=True)


def evidence(label: str, detail: str) -> None:
    print(f"  [evidence] {label}: {detail}", flush=True)


def req(port, method, path, body=b"", ctype="application/json", timeout=4.0):
    """One HTTP request. Returns (status, body, elapsed). status=='TIMEOUT'
    means the server produced no response within the timeout."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    started = time.monotonic()
    try:
        conn.request(method, path, body=body, headers={"Content-Type": ctype})
        resp = conn.getresponse()
        payload = resp.read().decode("utf-8", "replace")
        return resp.status, payload, time.monotonic() - started
    except (socket.timeout, TimeoutError):
        return "TIMEOUT", f"no response within {timeout}s", time.monotonic() - started
    except (ConnectionError, http.client.HTTPException) as exc:
        return "ERROR", f"{type(exc).__name__}: {exc}", time.monotonic() - started
    finally:
        conn.close()


def show(method, path, result, note=""):
    status, body, elapsed = result
    short = body if len(body) <= 160 else body[:157] + "..."
    suffix = f"  ({note})" if note else ""
    print(
        f"  {method:4s} {path:52s} -> {status} in {elapsed:.2f}s{suffix}\n"
        f"       {short}",
        flush=True,
    )


class Server:
    def __init__(self, name, port, root, shim=False):
        self.name, self.port, self.root = name, port, Path(root)
        env = dict(os.environ)
        env["PYTHONPATH"] = f"{SHIM}:{SRC}" if shim else SRC
        env["FRAME_PROXY_DATA_DIR"] = str(self.root)
        env["PYTHONFAULTHANDLER"] = "1"  # dump all thread stacks on SIGABRT
        env["FP_FAULT_TRIGGER"] = str(TRIGGER)
        self.root.mkdir(parents=True, exist_ok=True)
        self.out = open(OUT / f"server-{name}.stdout.log", "wb")
        self.err = open(OUT / f"server-{name}.stderr.log", "wb")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "frame_proxy", "serve",
             "--host", "127.0.0.1", "--port", str(port)],
            env=env, stdout=self.out, stderr=self.err,
        )
        SERVERS.append(self)
        self.wait_ready()

    @property
    def pid(self):
        return self.proc.pid

    def wait_ready(self, timeout=15.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status, _, _ = req(self.port, "GET", "/health", timeout=1.0)
            if status == 200:
                return
            time.sleep(0.2)
        raise RuntimeError(f"server {self.name} did not become ready")

    def threads(self):
        return sorted(int(p.name) for p in Path(f"/proc/{self.pid}/task").iterdir())

    def wchans(self):
        rows = []
        for tid in self.threads():
            try:
                wchan = Path(f"/proc/{self.pid}/task/{tid}/wchan").read_text().strip()
            except OSError:
                wchan = "?"
            rows.append((tid, wchan))
        return rows

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)


def put_clip(port, clip_id, avi, timeout=4.0):
    return req(port, "PUT", f"/v1/clips/{clip_id}", avi, "application/x-avi", timeout)


def post_inspection(port, clip_id, inspection_id, timeout=4.0):
    body = json.dumps({
        "inspection_id": inspection_id,
        "sensitivity": 0.55,
        "sample_stride": 1,
        "min_scene_frames": 2,
    }).encode()
    return req(port, "POST", f"/v1/clips/{clip_id}/inspections", body,
               "application/json", timeout)


def lslocks_for(pid):
    out = subprocess.run(["lslocks", "-p", str(pid), "-o", "PID,COMMAND,PATH"],
                         capture_output=True, text=True)
    lines = [ln for ln in out.stdout.splitlines() if ln.strip()]
    return lines[1:] if len(lines) > 1 else []


def abort_and_collect_stacks(server, expect_marker):
    """SIGABRT -> faulthandler dumps every thread's stack to stderr."""
    os.kill(server.pid, signal.SIGABRT)
    server.proc.wait(timeout=10)
    time.sleep(0.3)
    dump = Path(OUT / f"server-{server.name}.stderr.log").read_text(errors="replace")
    hits = dump.count(expect_marker)
    evidence(
        f"faulthandler dump of server {server.name}",
        f"'{expect_marker}' appears {hits} time(s); full dump: "
        f"out/server-{server.name}.stderr.log",
    )
    return dump


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    DATA.mkdir(parents=True, exist_ok=True)

    banner("PHASE 0  setup: generate footage with the production AVI writer")
    subprocess.run(
        [sys.executable, "-m", "frame_proxy", "demo-avi", "--output", str(AVI)],
        env={**os.environ, "PYTHONPATH": SRC}, check=True, capture_output=True,
    )
    avi = AVI.read_bytes()
    evidence("sample AVI", f"{len(avi)} bytes written by `frame_proxy demo-avi`")

    # ------------------------------------------------------------------
    banner("PHASE 1  server A baseline (data root rootA, no fault injection)")
    a = Server("A", 18765, DATA / "rootA")
    evidence("server A", f"pid={a.pid} threads={len(a.threads())}")
    show("PUT", "/v1/clips/clip-base-a", put_clip(a.port, "clip-base-a", avi))
    show("GET", "/v1/clips/clip-base-a", req(a.port, "GET", "/v1/clips/clip-base-a"))
    show("GET", "/health", req(a.port, "GET", "/health"))
    threads_a0 = len(a.threads())

    # ------------------------------------------------------------------
    banner("PHASE 2  storage anomaly strikes one key: the lock path for "
           "clip-stuck-a becomes a DIRECTORY")
    # DataPaths.lock_path("clip", "clip-stuck-a") == locks/clip-clip-stuck-a.lock
    lockdir = a.root / "locks" / "clip-clip-stuck-a.lock"
    lockdir.mkdir()
    evidence("injected fault",
             f"{lockdir} is a directory; open('a+b') on it raises "
             "IsADirectoryError (a real OSError from the storage layer)")
    show("PUT", "/v1/clips/clip-stuck-a  (first request, fault active)",
         put_clip(a.port, "clip-stuck-a", avi), "expect 500 storage_failure")
    show("GET", "/v1/clips/clip-stuck-a", req(a.port, "GET", "/v1/clips/clip-stuck-a"),
         "nothing was published")
    evidence("data root", "clips/ now contains: "
             + ", ".join(sorted(p.name for p in (a.root / 'clips').iterdir())))

    # ------------------------------------------------------------------
    banner("PHASE 3  fault cleared (directory removed); storage is healthy again")
    lockdir.rmdir()
    evidence("locks dir", "contents: "
             + ", ".join(sorted(p.name for p in (a.root / 'locks').iterdir())))
    show("PUT", "/v1/clips/clip-free-b  (unrelated id)",
         put_clip(a.port, "clip-free-b", avi), "unrelated id still works")

    # ------------------------------------------------------------------
    banner("PHASE 4  the wedge: every later write for clip-stuck-a hangs")
    show("PUT", "/v1/clips/clip-stuck-a  (retry #1, fault already cleared)",
         put_clip(a.port, "clip-stuck-a", avi), "expect TIMEOUT")
    show("PUT", "/v1/clips/clip-stuck-a  (retry #2)",
         put_clip(a.port, "clip-stuck-a", avi),
         "expect TIMEOUT, NOT 409 conflict")
    threads_a1 = len(a.threads())
    evidence("thread count",
             f"before wedge={threads_a0}, after 2 stuck PUTs={threads_a1} "
             f"(+{threads_a1 - threads_a0} permanently parked threads)")

    # ------------------------------------------------------------------
    banner("PHASE 5  why the requests never end: parked on an in-process lock")
    for tid, wchan in a.wchans():
        evidence(f"thread {tid}", f"wchan={wchan!r} (futex => threading.Lock, "
                                   "not a flock syscall)")
    held = lslocks_for(a.pid)
    evidence("lslocks -p A",
             f"{held if held else 'no file locks held by server A at all'}")
    show("GET", "/health", req(a.port, "GET", "/health"))
    show("GET", "/v1/clips/clip-base-a", req(a.port, "GET", "/v1/clips/clip-base-a"))
    show("GET", "/v1/clips/clip-stuck-a", req(a.port, "GET", "/v1/clips/clip-stuck-a"),
         "reads never take the lock")
    show("PUT", "/v1/clips/clip-free-c  (another unrelated id)",
         put_clip(a.port, "clip-free-c", avi))

    # ------------------------------------------------------------------
    banner("PHASE 6  second process B, SAME data root: the wedge is process-local")
    b = Server("B", 18766, DATA / "rootA")
    show("PUT", "B: /v1/clips/clip-stuck-a", put_clip(b.port, "clip-stuck-a", avi),
         "B has its own _THREAD_LOCKS and no flock is held by A")
    show("GET", "A: /v1/clips/clip-stuck-a", req(a.port, "GET", "/v1/clips/clip-stuck-a"),
         "A now reads B's committed record")
    show("PUT", "A: /v1/clips/clip-stuck-a  (object exists now!)",
         put_clip(a.port, "clip-stuck-a", avi),
         "still TIMEOUT -- the leaked lock is checked before the 409 check")
    evidence("thread count", f"server A now has {len(a.threads())} threads")

    # ------------------------------------------------------------------
    banner("PHASE 7  stack proof: SIGABRT server A, faulthandler dumps all threads")
    dump = abort_and_collect_stacks(a, "lock.py\", line 31 in __enter__")
    stuck = [ln.strip() for ln in dump.splitlines()
             if "lock.py" in ln and "__enter__" in ln]
    for ln in stuck[:3]:
        evidence("parked frame", ln)

    # ------------------------------------------------------------------
    banner("PHASE 8  restart A with the same data root: behaviour changes")
    a2 = Server("A2", 18765, DATA / "rootA")
    show("PUT", "/v1/clips/clip-stuck-a", put_clip(a2.port, "clip-stuck-a", avi),
         "fresh process: instant 409 (B created it), no hang")
    show("PUT", "/v1/clips/clip-new-d", put_clip(a2.port, "clip-new-d", avi),
         "fresh process: new id works")
    a2.stop()

    # ------------------------------------------------------------------
    banner("PHASE 9  scenario B: fault at the 3rd lock acquisition of one "
           "inspection (server C, shim)")
    c = Server("C", 18767, DATA / "rootC", shim=True)
    show("PUT", "/v1/clips/clip-scan-a", put_clip(c.port, "clip-scan-a", avi))
    TRIGGER.write_text("inspection-pending-a 2 ex")
    evidence("armed fault",
             "let 2 flock(LOCK_EX) pass on inspection-pending-a, fault the 3rd "
             "(the final 'indexed' commit)")
    show("POST", "/v1/clips/clip-scan-a/inspections {inspection-pending-a}",
         post_inspection(c.port, "clip-scan-a", "inspection-pending-a"),
         "the FIRST request itself hangs inside _mark_failed")
    status, body, _ = req(c.port, "GET", "/v1/inspections/inspection-pending-a")
    show("GET", "/v1/inspections/inspection-pending-a", (status, body, 0),
         "record is published as 'scanning' and can never advance")
    show("POST", "same inspection id again",
         post_inspection(c.port, "clip-scan-a", "inspection-pending-a"),
         "wedged id: TIMEOUT")
    show("POST", "different id {inspection-ok-b}",
         post_inspection(c.port, "clip-scan-a", "inspection-ok-b"),
         "unrelated id completes the whole scan")
    dump = abort_and_collect_stacks(c, "_mark_failed")
    chain = [ln.strip() for ln in dump.splitlines()
             if "inspections.py" in ln or "lock.py" in ln][:6]
    for ln in chain:
        evidence("parked chain", ln)

    # ------------------------------------------------------------------
    banner("PHASE 10  scenario D: fault at __exit__ (LOCK_UN) -> 500 but "
           "fully committed (server D, shim)")
    d = Server("D", 18768, DATA / "rootD", shim=True)
    TRIGGER.write_text("clip-half-a 0 un")
    evidence("armed fault", "fault the flock(LOCK_UN) for clip-half-a once")
    show("PUT", "/v1/clips/clip-half-a", put_clip(d.port, "clip-half-a", avi),
         "client sees 500 storage_failure ...")
    show("GET", "/v1/clips/clip-half-a", req(d.port, "GET", "/v1/clips/clip-half-a"),
         "... yet record AND media were committed")
    show("PUT", "/v1/clips/clip-half-a  (retry)",
         put_clip(d.port, "clip-half-a", avi),
         "wedged: TIMEOUT instead of the deserved 409")
    evidence("lslocks -p D",
             f"{lslocks_for(d.pid) or 'no file locks held (abandoned handle was reclaimed)'}")

    banner("ALL PHASES DONE -- artifacts under /tmp/frame-proxy-diag/out")


if __name__ == "__main__":
    try:
        main()
    finally:
        for srv in SERVERS:
            try:
                srv.stop()
            except Exception:
                pass
