import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for CloudWatch / Kibana."""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level": record.levelname.lower(),
            "service": getattr(record, "service", "unknown"),
            "message": record.getMessage(),
        }

        extra = getattr(record, "extra_fields", {})
        log_entry.update(extra)

        return json.dumps(log_entry, default=str)


class StructuredLogger:
    """Structured JSON logger wrapping Python's logging module.

    Usage:
        logger = get_logger("report_handler")
        logger.info("Processing report", report_id=42, batch_no=20260303)

        # bind() returns a child logger with persistent context
        child = logger.bind(batch_no=20260303, report_id=42)
        child.info("Status transition", status="IN_PROGRESS")
    """

    def __init__(self, service, context=None, _logger=None):
        self._service = service
        self._context = context or {}

        if _logger is not None:
            self._logger = _logger
        else:
            self._logger = logging.getLogger(f"structured.{service}")
            self._logger.setLevel(logging.DEBUG)
            self._logger.propagate = False

            if not self._logger.handlers:
                handler = logging.StreamHandler(sys.stdout)
                handler.setFormatter(JsonFormatter())
                self._logger.addHandler(handler)

    def bind(self, **kwargs):
        """Return a new StructuredLogger with merged context fields."""
        merged = {**self._context, **kwargs}
        return StructuredLogger(self._service, context=merged, _logger=self._logger)

    def _log(self, level, message, **kwargs):
        merged = {**self._context, **kwargs}
        extra = {"service": self._service, "extra_fields": merged}
        self._logger.log(level, message, extra=extra)

    def info(self, message, **kwargs):
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message, **kwargs):
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message, **kwargs):
        self._log(logging.ERROR, message, **kwargs)

    def debug(self, message, **kwargs):
        self._log(logging.DEBUG, message, **kwargs)


def get_logger(service):
    """Create a StructuredLogger for the given service name."""
    return StructuredLogger(service)
