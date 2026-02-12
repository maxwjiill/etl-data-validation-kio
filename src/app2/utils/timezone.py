import os
import time


def set_moscow_timezone():
    """Force localtime to Europe/Moscow so logs show UTC+3."""
    if os.environ.get("TZ") != "Europe/Moscow":
        os.environ["TZ"] = "Europe/Moscow"
        try:
            time.tzset()
        except AttributeError:
            pass
