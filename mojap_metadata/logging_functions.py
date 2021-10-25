import logging
import io

loggers = {}

def logging_setup() -> logging.Logger:

    global loggers

    if loggers.get("root"):
        return loggers.get("root")
    else:
        log = logging.getLogger("root")
        log.setLevel(logging.DEBUG)

        log_stringio = io.StringIO()
        handler = logging.StreamHandler(log_stringio)

        log_formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(log_formatter)
        log.addHandler(handler)

        # Add console output
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(log_formatter)
        log.addHandler(console)
        loggers["root"] = log
        return log
