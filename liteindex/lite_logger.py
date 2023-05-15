import logging
import sys
import uuid
import functools
from .defined_index import DefinedIndex
import inspect


class LiteLogger:
    LEVEL_COLORS = {
        logging.INFO: "\033[32m",  # Green
        logging.WARNING: "\033[33m",  # Yellow
        logging.ERROR: "\033[31m",  # Red
        logging.CRITICAL: "\033[35m",  # Magenta
    }

    LOG_LEVELS = {
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    def __init__(self, app_name, app_tags=[], min_log_level="info"):
        log_level = self.LOG_LEVELS[min_log_level.lower()]

        self.app_tags = app_tags
        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(log_level)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(log_level)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(log_name)s - %(file_name)s:%(line_no)d - %(message)s"
        )
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        self.log_index = DefinedIndex(
            app_name,
            schema={
                "name": "",
                "file_name": "",
                "line_number": 0,
                "log_type": "",
                "time": 0,
                "tags": [],
                "logs": [],
                "app_tags": [],
            },
        )

    def _log(self, level, name, id="", tags=[], *args, **kwargs):
        msg = ", ".join(str(arg) for arg in args)
        frame = inspect.stack()[2]
        file_name, line_number, _, _, _ = inspect.getframeinfo(frame.frame)

        self.logger.log(
            level,
            f"{id} - {tags} - {msg}",
            extra={"log_name": name, "file_name": file_name, "line_no": line_number},
        )

        log_data = {
            "name": name,
            "file_name": file_name,
            "line_number": line_number,
            "log_type": logging.getLevelName(level),
            "time": kwargs.get("time", 0),
            "tags": tags,
            "logs": msg,
            "app_tags": self.app_tags,
        }
        self.log_index.set(str(uuid.uuid4()), log_data)

    def info(self, name, id="", tags=[], *args, **kwargs):
        print("name", name)
        print("id", id)
        print("tags", tags)
        print("args", *args)
        print("kwargs", **kwargs)

        sys.stdout.write(self.LEVEL_COLORS[logging.INFO])
        self._log(logging.INFO, name, id, tags, *args, **kwargs)
        sys.stdout.write("\033[0m")

    def warning(self, name, id="", tags=[], *args, **kwargs):
        sys.stdout.write(self.LEVEL_COLORS[logging.WARNING])
        self._log(logging.WARNING, name, id, tags, *args, **kwargs)
        sys.stdout.write("\033[0m")

    def error(self, name, id="", tags=[], *args, **kwargs):
        sys.stdout.write(self.LEVEL_COLORS[logging.ERROR])
        self._log(logging.ERROR, name, id, tags, *args, exc_info=True, **kwargs)
        sys.stdout.write("\033[0m")

    def exception(self, name, id="", tags=[], *args, **kwargs):
        sys.stdout.write(self.LEVEL_COLORS[logging.ERROR])
        self._log(logging.ERROR, name, id, tags, *args, exc_info=True, **kwargs)
        sys.stdout.write("\033[0m")

    def critical(self, name, id="", tags=[], *args, **kwargs):
        sys.stdout.write(self.LEVEL_COLORS[logging.CRITICAL])
        self._log(logging.CRITICAL, name, id, tags, *args, **kwargs)
        sys.stdout.write("\033[0m")


if __name__ == "__main__":
    import random
    import time

    def simulate_exception():
        try:
            1 / 0
        except ZeroDivisionError:
            logger.exception(
                "Simulation",
                "DIV_BY_ZERO",
                ["exception_example"],
                "An exception occurred while dividing by zero",
            )

    logger = LiteLogger("MyApp", app_tags=["application", "demo"])

    # Info logging
    logger.info(
        "Example", "INFO_LOG", ["tag1", "tag2"], "This is an info log with two tags"
    )

    # Warning logging
    logger.warning(
        "Example", "WARNING_LOG", ["tag3"], "This is a warning log with one tag"
    )

    # Error logging
    logger.error(
        "Example", "ERROR_LOG", ["tag4", "tag5"], "This is an error log with two tags"
    )

    # Exception logging
    simulate_exception()

    # Sleep for a while to demonstrate time in logs
    time.sleep(2)

    # Another example of info logging
    logger.info(
        "RandomNumber",
        "RND_NUM_LOG",
        [],
        "Generated random number:",
        random.randint(1, 100),
    )
