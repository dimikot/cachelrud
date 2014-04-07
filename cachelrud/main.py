import os
import sys
import socket
import glob
import time
import re
import human_bytes
import getopt
import daemon_helper
from ConfigParser import ConfigParser
from multiprocessing.queues import Empty as QueueEmpty, Full as QueueFull
from multiprocessing import Queue, Process


ERROR_RECOVER_RECHECK_DT = 20
PARENT_CHECK_TIMEOUT = 2
CONFIGS = []
DEFAULT_SECTION = "DEFAULT"
UDP_BUF_SIZE = 10240
UPDATER_QUEUE_MAX_SIZE = 1000


def main():
    """
    :rtype: None
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "debug", "name=", "pidfile="])
    except getopt.GetoptError as err:
        print str(err)
        return usage()
    if len(args) != 1:
        return usage("Config file name is missing.")

    # Collect command-line args.
    opt_config = args[0]
    opt_name = "cachelrud"
    opt_debug = None
    opt_pidfile = None
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o == "--name":
            opt_name = a
        elif o == "--debug":
            opt_debug = True
        elif o == "--pidfile":
            opt_pidfile = a
        else:
            return usage("Unhandled option " + o)

    # Parse config.
    conf = parse_config([
        os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + "/..") + "/cachelrud.conf",
        opt_config
    ])

    # Deal with logging & daemon options.
    daemon_helper.set_process_name(opt_name)
    log = daemon_helper.create_log(
        proc_name=opt_name,
        log_type=conf.get(DEFAULT_SECTION, "log_type") if opt_pidfile is not None else "stdout",
        log_syslog_addr=conf.get(DEFAULT_SECTION, "log_syslog_addr"),
        log_syslog_facility=conf.get(DEFAULT_SECTION, "log_syslog_facility"),
        log_file_path=conf.get(DEFAULT_SECTION, "log_file_path")
    )
    import logging
    if opt_debug is not None or conf.getint(DEFAULT_SECTION, "is_debug"):
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    # Run main loop.
    if opt_pidfile is not None:
        daemon_helper.exceptions_to_log(log, loop_watchdog_daemon)(log, conf, opt_pidfile, conf.get(DEFAULT_SECTION, 'setuid'))
    else:
        daemon_helper.exceptions_to_log(log, loop_watchdog)(log, conf)


def usage(msg=None):
    """
    :type msg: str
    :rtype: None
    """
    if msg:
        print msg
    print "\n".join((
        "Usage: " + sys.argv[0] + " [OPTION]... /path/to/config/file.conf",
        "All arguments are optional:",
        "    -h, --help            print this help and exit",
        "    --debug               increase verbosity",
        "    --name=process_name   set a name for the daemon process",
        "    --pidfile=...         if set, detach and save daemon PID to this file",
        "",
        "Config file should contain [DEFAULT] section and one or more",
        "destination database sections to work with."
    ))
    sys.exit(1)


def parse_config(configs):
    """
    :type configs list
    :rtype: ConfigParser
    """
    conf = ConfigParser()

    all_configs = []
    while len(configs) > 0:
        all_configs += configs
        files = []
        for mask in configs:
            for f in glob.glob(mask):
                if os.path.isfile(f):
                    files.append(f)
        conf.read(files)
        configs = []
        if conf.has_option(DEFAULT_SECTION, "include"):
            configs = list(set(re.split(r'\s+', conf.get(DEFAULT_SECTION, "include"))) - set(all_configs))

    for section in conf.sections():
        for k, v in conf.items(DEFAULT_SECTION):
            if not conf.has_option(section, k):
                conf.set(section, k, v)
        for k, v in conf.items(section):
            v = re.sub(r'^\s*"|"\s*$', '', v) # remove quotes
            conf.set(section, k, v)

    conf.remove_section(DEFAULT_SECTION)

    if not conf.sections():
        usage("No sections found in config files " + ", ".join(all_configs))
    return conf


def loop_watchdog_daemon(log, conf, pidfile, setuid):
    """
    :type log: logging.Logger
    :type conf: ConfigParser
    :type pidfile: str
    :type setuid: str
    :rtype: None
    """
    daemon_helper.switch_to_daemon_mode(
        pidfile=pidfile,
        setuid=conf.get(DEFAULT_SECTION, 'setuid')
    )
    loop_watchdog(log, conf)


def loop_watchdog(log, conf):
    """
    :type conf: ConfigParser
    :rtype: None
    """
    listeners = {}
    updater = None
    reaper = None
    updater_queue = Queue(UPDATER_QUEUE_MAX_SIZE)
    log.info("Running watchdog loop")
    while True:
        try:
            for section in conf.sections():
                # Respawn listeners.
                listenhost = conf.get(section, "listenhost")
                listenport = conf.getint(section, "listenport")
                key = listenhost + ":" + str(listenport)
                if key not in listeners or not listeners[key].is_alive():
                    if key in listeners:
                        log.warning("Listener %d died unexpectedly, respawning.", listeners[key].pid)
                    listeners[key] = Process(target=daemon_helper.exceptions_to_log(log, loop_listener), args=(
                        log.getChild("listener"),
                        listenhost, listenport,
                        updater_queue,
                        conf.sections(),
                        conf.getint(section, 'bucket_size'),
                        conf.getint(section, 'bucket_flush_max_time')
                    ))
                    listeners[key].start()
                    log.info("Spawned a new listener for %s:%d: pid=%d", listenhost, listenport, listeners[key].pid)
                # Respawn updater.
                if updater is None or not updater.is_alive():
                    if updater is not None:
                        log.warning("Updater %d died unexpectedly, respawning.", updater.pid)
                    updater = Process(target=daemon_helper.exceptions_to_log(log, loop_updater), args=(
                        log.getChild("updater"),
                        conf,
                        updater_queue,
                    ))
                    updater.start()
                    log.info("Spawned a new updater: pid=%d", updater.pid)
                # Respawn reaper.
                if reaper is None or not reaper.is_alive():
                    if reaper is not None:
                        log.warning("Reaper %d died unexpectedly, respawning.", reaper.pid)
                    reaper = Process(target=daemon_helper.exceptions_to_log(log, loop_reaper), args=(
                        log.getChild("reaper"),
                        conf,
                    ))
                    reaper.start()
                    log.info("Spawned a new reaper: pid=%d", reaper.pid)
        except Exception, e:
            log.error("%s: %s", str(e.__class__.__name__), str(e))
        time.sleep(1)


def loop_listener(log, listenhost, listenport, updater_queue, allowed_sections, bucket_size, bucket_flush_max_time):
    """
    :type log: logging.Logger
    :type listenhost: str
    :type listenport: int
    :type updater_queue: Queue
    :type allowed_sections: list
    :type bucket_size: int
    :rtype: None
    """
    daemon_helper.set_process_name(log.name)
    ppid = os.getppid()
    log.info("Listening at %s:%d", listenhost, listenport)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((listenhost if listenhost != "*" else "0.0.0.0", listenport))
    sock.settimeout(PARENT_CHECK_TIMEOUT)
    buckets = {k: {} for k in allowed_sections}
    last_queue_put_at = time.time()

    def flush_buckets(sec):
        bucket = buckets[sec]
        if not bucket:
            return
        try:
            updater_queue.put_nowait((sec, bucket.keys()))
        except QueueFull:
            log.error("Queue is full, possibly updater process is dead?")
            log.error("Because of that all keys (%d) are discarded.", len(bucket))
        buckets[sec] = {}

    while True:
        try:
            data, addr = sock.recvfrom(UDP_BUF_SIZE)
        except socket.timeout:
            if not check_parent_running(log, ppid):
                return
            if time.time() > last_queue_put_at + bucket_flush_max_time:
                last_queue_put_at = time.time()
                for section in allowed_sections:
                    flush_buckets(section)
            continue
        if not data:
            continue

        log.debug("Received a datagram from %s:%s", addr[0], addr[1])
        for line in data.split("\n"):
            line = line.strip()
            if not line:
                continue
            log.debug("<-- %s", line)
            try:
                section, key = line.split(":", 2)
                if section not in allowed_sections:
                    log.warning("Unknown section name: '%s'", section)
                    continue
                buckets[section][key] = 1
                if len(buckets[section]) >= bucket_size:
                    last_queue_put_at = time.time()
                    flush_buckets(section)
            except ValueError:
                log.info("Message should have format 'section_name:id', but '%s' received", line)


def loop_updater(log, conf, updater_queue):
    """
    :type log: logging.Logger
    :type conf: ConfigParser
    :type updater_queue: Queue
    :rtype: None
    """
    daemon_helper.set_process_name(log.name)
    ppid = os.getppid()
    storages = {}
    while True:
        try:
            section, keys = updater_queue.get(True, PARENT_CHECK_TIMEOUT)
        except QueueEmpty:
            if not check_parent_running(log, ppid):
                return
            continue
        log_section = log.getChild(section)
        log_section.debug("Received a command to touch %d keys", len(keys))
        log_section.debug("Keys: %s", keys)
        if section not in storages:
            storages[section] = get_storage(log, dict(conf.items(section)))
        log_section.debug("Touching %d keys", len(keys))
        t0 = time.time()
        storages[section].touch_keys(keys)
        dt = time.time() - t0
        log_section.info("Touched %d keys, took %d ms", len(keys), int(dt * 1000))


def loop_reaper(log, conf):
    """
    :type log: logging.Logger
    :type conf: ConfigParser
    """
    daemon_helper.set_process_name(log.name)
    ppid = os.getppid()
    storages = {}
    next_round_time = {}
    while True:
        time.sleep(1)
        if not check_parent_running(log, ppid):
            return

        for section in conf.sections():
            t0 = time.time()
            if section in next_round_time and t0 < next_round_time[section]:
                continue
            log_section = log.getChild(section)
            if section not in storages:
                storages[section] = get_storage(log_section, dict(conf.items(section)))
            storage = storages[section]
            if not storage.can_write():
                log_section.debug(
                    "This connection is not writable. Possibly the node is not master, so retry in %d seconds.",
                    ERROR_RECOVER_RECHECK_DT
                )
                del storages[section]
                next_round_time[section] = t0 + ERROR_RECOVER_RECHECK_DT
                continue
            log_section.debug("Getting stats")
            try:
                size, count = storage.get_stat()
            except Exception, e:
                log_section.error("%s: %s", e.__class__.__name__, str(e))
                del storages[section]
                next_round_time[section] = t0 + ERROR_RECOVER_RECHECK_DT
                continue
            log_section.debug("Stats: size=%d, count=%d", size, count)
            count = max(count, 1)  # to avoid division by zero
            avg_size = max(size * 1.0 / count, 1)  # to avoid division by zero
            maxsize = human_bytes.human2bytes(conf.get(section, 'maxsize'))
            num_del = int((size - maxsize) / avg_size)
            if num_del <= 0:
                dt = conf.getfloat(section, 'reaper_recheck_sleep')
                log_section.debug("No need to reap anything, will recheck in %d seconds", dt)
                next_round_time[section] = t0 + dt
                continue
            num_del = max(num_del, conf.getint(section, 'reaper_one_round_min'))
            num_del = min(num_del, conf.getint(section, 'reaper_one_round_max'))
            log_section.debug("Performing one round of LRU removal for %d keys", num_del)
            num_real_del = storage.clean_oldest(num_del)
            dt = time.time() - t0
            log_section.info("Removed %d keys, took %d ms", num_real_del, int(dt * 1000))
            next_round_time[section] = t0 + conf.getfloat(section, 'reaper_small_sleep_between_rounds')

        if len(next_round_time) > 0:
            closest_time = min(next_round_time.values())
            dt = closest_time - time.time()
            if dt > 0.01:
                time.sleep(min(dt, PARENT_CHECK_TIMEOUT))
        else:
            # No sections found, why?
            time.sleep(PARENT_CHECK_TIMEOUT)


def get_storage(log, params):
    """
    :type log: logging.Logger
    :type params: dict
    :rtype: cachelrud.storage.Base
    """
    dsn = params['dsn']
    m = re.match(r'^(\w+)://', dsn)
    if not m:
        raise Exception("DSN must have a protocol specified, but '%s' given" % dsn)
    storage_name = m.group(1)
    pkg = getattr(__import__(__package__ + ".storage." + storage_name).storage, storage_name)
    return pkg.Storage.get_instance(log, params)


def check_parent_running(log, ppid):
    """
    :type log: logging.Logger
    :type ppid: int
    :rtype: bool
    """
    try:
        os.kill(ppid, 0)
        return True
    except OSError:
        log.error("Parent process %d died, exiting", ppid)
        return False
