import os
import pwd
import sys
import logging
import logging.handlers
import traceback


_process_name = None


def create_log(proc_name, log_type, log_syslog_addr, log_syslog_facility, log_file_path):
    """
    :type proc_name: str
    :type log_type: str
    :type log_syslog_addr: str
    :type log_syslog_facility: str
    :type log_file_path: str
    :rtype: logging.Logger
    """
    log = MyLogger(proc_name)
    if log_type == "syslog":
        handler = SysLogHandlerDebugToInfo(address=log_syslog_addr, facility=log_syslog_facility)
    elif log_type == "file":
        handler = logging.handlers.WatchedFileHandler(filename=log_file_path)
    elif log_type == "stdout":
        handler = logging.StreamHandler(sys.stdout)
    else:
        raise Exception("Unknown log type: " + log_type)
    handler.setFormatter(logging.Formatter("%(name)s[%(process)d]: %(levelname)s: %(message)s"))
    log.addHandler(handler)
    return log


def switch_to_daemon_mode(pidfile, setuid):
    """
    :type pidfile: str
    :type setuid: str
    :rtype: None
    """
    pid = os.fork()
    if pid == 0:
        os.setsid()
        pid = os.fork()
        if pid == 0:
            os.chdir("/")
            os.umask(0)
        else:
            os._exit(0)
    else:
        os._exit(0)

    with open(pidfile, 'w') as f:
        f.write(str(os.getpid()))

    # Close only stdin, stdout, stderr: other handlers may be opened and good
    # (e.g. syslog connection, log files etc.).
    for fd in range(0, 2):
        try:
            os.close(fd)
        except OSError:
            pass
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)

    setuid_info = pwd.getpwnam(setuid)
    os.setgid(setuid_info.pw_gid)  # must be BEFORE setuid()!
    os.setuid(setuid_info.pw_uid)


def set_process_name(name):
    """
    :type name: str
    :rtype: None
    """
    global _process_name
    _process_name = name
    if sys.platform == 'linux2':
        import ctypes
        libc = ctypes.cdll.LoadLibrary('libc.so.6')
        libc.prctl(15, _process_name, 0, 0, 0)


def exceptions_to_log(log, fn):
    """
    :type log: logging.Logger
    :type fn: (*args) -> None
    :rtype: (*args) -> None
    """
    def wrapper(*args, **kwargs):
        try:
            fn(*args, **kwargs)
        except KeyboardInterrupt:
            log.info("SIGINT received")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error("%s: %s", exc_type.__name__, exc_value)
            log.debug("Exception details:\n%s", "".join(traceback.format_tb(exc_traceback)))
    return wrapper


# Logger class wrapper.
# Unfortunately python's standard logging.Logger seems to be quite dumb...
class MyLogger(logging.Logger):
    # Returns a child logger with SAME handler and level as the current one.
    def getChild(self, suffix):
        child = logging.Logger.getChild(self, suffix)
        child.setLevel(self.level)
        for h in self.handlers:
            child.addHandler(h)
        return child

    # Replaces one call to logger with one multi-line string into multiple calls.
    # This is needed to achieve nice syslog messages (all lines should be prefixed).
    def _log(self, level, msg, args, exc_info=None, extra=None):
        msg = msg % args
        for line in msg.split("\n"):
            logging.Logger._log(self, level, line, (), exc_info, extra)


# Forces debug-level messages to be sent with INFO syslog priority
# to put them to the same log file by default (not to /var/log/debug).
class SysLogHandlerDebugToInfo(logging.handlers.SysLogHandler):
    def encodePriority(self, facility, priority):
        if isinstance(priority, basestring):
            priority = self.priority_names[priority]
        if priority <= logging.DEBUG:
            priority = logging.INFO
        return logging.handlers.SysLogHandler.encodePriority(self, facility, priority)
