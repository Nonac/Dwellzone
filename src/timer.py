"""Process-level cumulative timer."""

import time

_process_start = None


def reset_timer():
    """Resets the global timer to the current time."""
    global _process_start
    _process_start = time.time()


def elapsed():
    """Returns seconds elapsed since the last reset_timer() call.

    Returns:
        Elapsed time in seconds as a float.
    """
    if _process_start is None:
        return 0.0
    return time.time() - _process_start
