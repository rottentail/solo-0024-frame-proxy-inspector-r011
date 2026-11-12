"""Fault-injection shim for diagnosis only.

This is NOT business code and the repository is never modified. It simulates
a transient storage fault by making fcntl.flock raise OSError(EIO) for one
targeted lock operation -- the same signature a failing disk / corrupted
lock namespace produces at the syscall boundary.

Armed at runtime through the trigger file named by FP_FAULT_TRIGGER.
Trigger file content:  "<path-substring> <skip> <op>"
  <path-substring>: only flock() calls whose file path contains this match
  <skip>:           number of matching calls to let through before faulting
  <op>:             "ex" faults an exclusive acquisition, "un" faults an unlock
The shim disarms itself (deletes the trigger file) after injecting once.
"""

import errno
import fcntl
import os

_real_flock = fcntl.flock
_trigger = os.environ.get("FP_FAULT_TRIGGER", "")


def _matching_path(fd: int, needle: str) -> bool:
    try:
        return needle in os.readlink("/proc/self/fd/%d" % fd)
    except OSError:
        return False


def _flock(fd: int, op: int) -> None:
    if _trigger:
        try:
            with open(_trigger, "r", encoding="utf-8") as handle:
                parts = handle.read().split()
        except OSError:
            parts = []
        if len(parts) >= 2:
            needle = parts[0]
            skip = int(parts[1])
            want_un = len(parts) > 2 and parts[2] == "un"
            is_ex = bool(op & fcntl.LOCK_EX)
            is_un = op == fcntl.LOCK_UN
            if ((want_un and is_un) or (not want_un and is_ex)) and _matching_path(
                fd, needle
            ):
                if skip > 0:
                    suffix = parts[2] if len(parts) > 2 else "ex"
                    with open(_trigger, "w", encoding="utf-8") as handle:
                        handle.write("%s %d %s" % (needle, skip - 1, suffix))
                else:
                    try:
                        os.unlink(_trigger)
                    except OSError:
                        pass
                    raise OSError(errno.EIO, "injected storage fault (flock)")
    return _real_flock(fd, op)


fcntl.flock = _flock
