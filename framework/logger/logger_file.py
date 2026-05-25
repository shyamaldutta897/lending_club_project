"""
Simple logging wrapper for use inside pipelines.

This module exposes a tiny `Log4j` class that delegates to the JVM-side
Log4j/Log4j2 logger available from a SparkSession's `_jvm` bridge. It is a
convenience wrapper so pipeline code can call `logger.info(...)`,
`logger.warn(...)`, and `logger.error(...)` without interacting with the JVM
API directly.

Notes:
- The Spark driver is responsible for bootstrapping the Log4j2 configuration
  file (see `framework/session/spark_session.py`), which sets the
  `log4j2.configurationFile` system property pointing to `configs/log4j2.properties`.
- This wrapper uses the JVM `org.apache.log4j.LogManager` API. If you migrate
  fully to Log4j2 native Java classes you may prefer `org.apache.logging.log4j.LogManager`.

Usage:

```python
from framework.logger.logger_file import Log4j
logger = Log4j(spark)
logger.info('hello')
```
"""


class Log4j(object):
    """Lightweight logger wrapper around Spark's JVM Log4j/Log4j2.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session; used to access the JVM logger via `spark._jvm`.
    """

    def __init__(self, spark):
        # Access the JVM logging classes through the SparkSession bridge.
        # We request the logger named "lending_club_app" so messages can be
        # filtered or routed via the Log4j2 configuration file.
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("lending_club_app")

    def error(self, message):
        """Log an error-level message.

        Parameters
        ----------
        message : str
            Message to log.
        """
        self.logger.error(message)

    def warn(self, message):
        """Log a warning-level message.

        Parameters
        ----------
        message : str
            Message to log.
        """
        # `warn` is kept for convenience to match common logging APIs.
        self.logger.warn(message)

    def info(self, message):
        """Log an info-level message.

        Parameters
        ----------
        message : str
            Message to log.
        """
        self.logger.info(message)
        