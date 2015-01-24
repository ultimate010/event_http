# -*-encoding=utf-8-*-
from __future__ import absolute_import, division, \
    with_statement
import signal
import time
import urlparse
import re
import eventloop
import errno
import socket
import random
import os
import logging
logger = logging.getLogger("EventHttp")
logger.setLevel(logging.INFO)
# logger.setLevel(logging.DEBUG)


class HttpHelper(object):
    def __init__(self):
        self.convert_table = [0 for x in range(256)]

    def generate_cookie(self, cookie):
        """Cookie is dict
        """
        ret = []
        has_unicode = False
        for k, v in cookie.items():
            if isinstance(k, unicode) or isinstance(v, unicode):
                has_unicode = True
            ret.append("%s=%s; " % (k, v))
        if has_unicode:
            return "".join(ret)[:-2].encode("utf-8") + "\r\n"
        else:
            return "".join(ret)[:-2] + "\r\n"

    def generate_http_message(self, method, path,
                              header, http_version="HTTP/1.1"):
        request_line = "%s %s %s\r\n" % (method, path, http_version)
        header_line = []
        for k, v in header.items():
            header_line.append("%s: %s\r\n" % (k, v))
        return request_line + "".join(header_line)


class RequestTask(object):
    def __init__(self, **kwargs):
        self.default_header = {
            "Connection": "close",
            "Cache-Control": "no-cache",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/39.0.2171.99 Safari/537.36",
            "Host": "",
        }
        self.task_info = {
            "method": "GET",
            "task_id": None,
            "url": None,
            "remote": None,
            "cookie": None,
            "callback": None,
            "socket": None,
            "proxy": None,
            "query_string": None,
            "header": self.default_header,
            "resp_header": None,
            "last_active_time": None,
            "retry": True,
            "status": None,
            "reason": None,
            "send_buf": "",
            "recv_buf": "",
        }
        for k, v in kwargs.items():
            self.task_info[k] = v

    def do_ig_ep(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            pass

    def __str__(self):
        return "url:%s\nmethon:%s\nreason:%s\nproxy:%s\nlast_active:%s" % \
            (self.task_info["url"],
             self.task_info["method"],
             self.task_info["reason"],
             self.task_info["proxy"],
             self.task_info["last_active_time"]
             )

    def destroy(self):
        self.task_info["last_active_time"] = None
        self.task_info["task_id"] = None
        self.task_info["send_buf"] = ""
        self.task_info["recv_buf"] = ""
        if self.task_info["socket"]:
            self.do_ig_ep(self.task_info["socket"].shutdown, socket.SHUT_RDWR)
            self.do_ig_ep(self.task_info["socket"].close)
            self.task_info["socket"] = None


class EventHttp(object):
    def __init__(self):
        self.eventloop = eventloop.EventLoop()
        self.running_tasks = {}
        self.failed_tasks = []
        self.socket2task = {}
        self.concurrent_tasks = 0
        self.helper = HttpHelper()
        self.METHOD_GET = "GET"
        self.HEADER_END = "\x0d\x0a\x0d\x0a"
        self.HTTPSTATUS = {
            "STATUS_CONNECT": 0x1 << 3,
            "STATUS_CONNECTED": 0x1 << 4,
            "STATUS_FAILED": 0x1 << 6,
            "STATUS_SEND": 0x1 << 7,
            "STATUS_RECV": 0x1 << 8,
            "STATUS_HEADER": 0x1 << 9,
            "STATUS_DONE": 0x1 << 10
        }
        self.conf = {
            "buf_size": 32 * 1024,
            "verbose": False,
            "task_timeout": 5,
            "max_concurrent_tasks": 1000,
        }

    def get_uni_id(self):
        while True:
            random_id = os.urandom(8).encode("hex")
            if random_id not in self.running_tasks:
                return random_id

    def _create_request_content(self, task):
        content = []
        parsed_url = urlparse.urlparse(task.task_info['url'])
        hostname = parsed_url.hostname
        port = parsed_url.port
        if not port:
            port = 80
        proxy_url = task.task_info["proxy"]
        if proxy_url:
            _url = urlparse.urlparse(proxy_url)
            if _url.scheme in "http":
                _hostname = _url.hostname
                _port = _url.port
                task.task_info["remote"] = (_hostname, _port)
            else:
                raise Exception("Can not support %s proxy" % _url["scheme"])
        if not task.task_info["remote"]:
            task.task_info["remote"] = (hostname, port)
        task.task_info["header"]["Host"] = "%s:%s" % (hostname, port)
        if proxy_url:
            pass
        if parsed_url.params:
            path = parsed_url.path + ";" +\
                parsed_url.params + "?" + parsed_url.query
        else:
            path = parsed_url.path + "?" + parsed_url.query
        if proxy_url:
            path = parsed_url.scheme + "://" + parsed_url.hostname + path
        method = task.task_info["method"]
        content.append(self.helper.
                       generate_http_message(method,
                                             path,
                                             task.task_info["header"],
                                             ))
        cookie = task.task_info['cookie']
        if cookie:
            content.append("Cookie: ")
            content.append(self.helper.generate_cookie(cookie))
        content.append("\r\n")  # Empty line
        return "".join(content)

    def _remove_task(self, task, why=None):
        task.task_info["reason"] = why
        logger.debug('Remove task %s' % task)
        task_id = task.task_info["task_id"]
        if task_id not in self.running_tasks:
            logger.error("Taks id not in tasks")
            return
        if why and task.task_info["retry"]:
            logger.debug("Remove %s" % why)
            self.failed_tasks.append(task)
        _socket = task.task_info["socket"]
        self.concurrent_tasks -= 1
        del self.running_tasks[task_id]
        if not _socket:
            # Not init socket
            return 
        try:
            try:
                self.eventloop.remove(_socket)
            except Exception as e:
                logger.error("Remove err %s %s" % (e, why))
            task.destroy()
            del self.socket2task[_socket]
            del _socket
        except Exception as e:
            logger.error("Destory err %s %s" % (e, why))

    def _create_socket_and_register(self, task):
        try:
            _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            _socket.setblocking(0)
            self.eventloop.add(_socket,
                               eventloop.POLL_OUT |
                               eventloop.POLL_ERR)
        except Exception as e:
            self._remove_task(task, why="Create socket: %s" % e)
            return None
        if not _socket:
            self._remove_task(task, why="Create socket err" )
            return None
        self.socket2task[_socket] = task
        task.task_info["socket"] = _socket
        logger.debug("Connect %s:%s" % task.task_info["remote"])
        try:
            _socket.connect(task.task_info["remote"])
        except socket.error as e:
            if e.errno != errno.EINPROGRESS:
                self._remove_task(task,
                                  why="Connect error: %s %s:%s" %
                                  (e, task.task_info["remote"]))
                return None
        return _socket

    def _add_task(self, task):
        try:
            req_content = self._create_request_content(task)
            logger.debug("\nReq content:\n%s " % req_content)
        except Exception as e:
            self._remove_task(task, why="Generate req content err %s" % e)
            return
        self._active_task(task)
        task.task_info["send_buf"] = req_content
        self._create_socket_and_register(task)

    def _active_task(self, task):
        task.task_info["last_active_time"] = time.time()

    def _sweep_timeout(self, signum=None, frame=None):
        current_time = time.time()
        if current_time - self.last_check_time > self.conf["task_timeout"]:
            self.last_check_time = current_time
            time_point = current_time - self.conf["task_timeout"]
            logger.debug("Sweep before %s" % time_point)
            del_tasks = []
            for k, v in self.running_tasks.items():
                if time_point > v.task_info["last_active_time"]:
                    del_tasks.append(v)
            for t in del_tasks:
                self._remove_task(t, why="Time out")

    def _handle_events(self, events):
        for _socket, fd, event in events:
            try:
                task = self.socket2task[_socket]
            except:
                import pdb
                pdb.set_trace()
            if task.task_info["task_id"] not in self.running_tasks:
                self._remove_task(task, why="Not valid task")
                raw_input()
                continue
            if event & eventloop.POLL_ERR:
                logger.debug("Event err hup")
                self._remove_task(task, why="Epoll err")
                continue
            if event & eventloop.POLL_OUT:
                logger.debug("Event out")
                self._send_data(task)
            if event & eventloop.POLL_IN:
                logger.debug("Event in")
                self._receive_data(task)
            if event & eventloop.POLL_HUP:
                logger.debug("Event hup")
                self._remove_task(task, why="Hup")

    def _send_data(self, task):
        self._active_task(task)
        _socket = task.task_info["socket"]
        send_buf = task.task_info["send_buf"]
        if not send_buf or not _socket:
            return True
        uncomplete = False
        try:
            s = _socket.send(send_buf)
            if s < len(send_buf):
                task.task_info["send_buf"] = send_buf[s:]
                uncomplete = True
        except (OSError, IOError) as e:
            error_no = eventloop.errno_from_exception(e)
            if error_no in (errno.EAGAIN, errno.EINPROGRESS,
                            errno.EWOULDBLOCK):
                uncomplete = True
            else:
                logger.error(e)
                if self.conf['verbose']:
                    import traceback
                    traceback.print_exc()
                self._remove_task(task, why="Send error")
                return False
        if uncomplete:
            event = eventloop.POLL_ERR | eventloop.POLL_OUT
            self.eventloop.modify(_socket, event)
        else:
            task.task_info["send_buf"] = ""
            event = eventloop.POLL_ERR | eventloop.POLL_IN
            self.eventloop.modify(_socket, event)
        return True

    def _receive_data(self, task):
        self._active_task(task)
        _socket = task.task_info["socket"]
        data = None
        try:
            data = _socket.recv(self.conf["buf_size"])
            logger.debug("Receive\n%s" % data)
        except (OSError, IOError) as e:
            if eventloop.errno_from_exception(e) in \
                    (errno.ETIMEDOUT, errno.EAGAIN, errno.EWOULDBLOCK):
                return
            else:
                self._remove_task(task, why="Receive err")
        if not data:
            func = task.task_info["callback"]
            try:
                func(task)
            except:
                self._remove_task(task, why="Call back err")
                return 
            else:
                logger.debug("Finish remove task")
                self._remove_task(task)  # Finish
            return
        task.task_info["recv_buf"] += data

    def _process_tasks(self):
        self.last_check_time = time.time()
        # signal.setitimer(signal.ITIMER_REAL, 1, 3)
        # signal.signal(signal.SIGALRM, self._sweep_timeout)
        while True:
            self.eventloop.single_run()
            self._sweep_timeout()
            if self.concurrent_tasks < 1:
                break
        # signal.setitimer(signal.ITIMER_REAL, 0, 0)

    def dispatch_tasks(self, tasks):
        self.failed_tasks = []
        for pos, t in enumerate(tasks):
            if t.task_info["task_id"]:
                continue
            task_id = self.get_uni_id()
            t.task_info["task_id"] = task_id
            self.running_tasks[task_id] = t
            self.concurrent_tasks += 1
            if self.concurrent_tasks < self.conf["max_concurrent_tasks"]:
                self._add_task(t)
            else:
                break
        self.failed_tasks.extend(tasks[pos + 1:])
        self.eventloop.add_handler(self._handle_events)
        self._process_tasks()
        self.eventloop.remove_handler(self._handle_events)
        return self.failed_tasks

    def do_until_done(self, tasks):
        from ip_pool.IpClient import IpClient
        ipc = IpClient()
        ips = ipc.getIps(100)
        while tasks:
            for task in tasks:
                ip = random.choice(ips.keys())
                port = int(ips[ip]) - 100000
                task.task_info["proxy"] = "http://%s:%s" % (ip, port)
            tasks = self.dispatch_tasks(tasks)
            logger.info("Loop after %s " % len(tasks))

task_count = 0

def cb(task):
    global task_count
    task_count += 1
    url = task.task_info["url"]
    print "Done %s %s" % (url, task_count)
    content = task.task_info["recv_buf"]
    u = re.findall(r'p=(.*)', url)[0]
    try:
        r = re.findall(r'"p": "(\d+)"', content)[0]
    except:
        return 
    if r != u:
        print "Req: %s " % u
        print "Rep: %s " % r
        print "%s Not Ok" % task
        import pdb
        pdb.set_trace()
    else:
        pass

if __name__ == "__main__":
    task = RequestTask(url="http://httpbin.org/get?p=%s" % random.random(),
                       method="GET",
                       callback=cb
                       )

    tasks = []
    # tasks.append(task)
    task = RequestTask(url="http://httpbin.org/get?p=%s" % random.random(),
                       method="GET",
                       callback=cb
                       )
    # tasks.append(task)
    for i in xrange(20000):
        task = RequestTask(url="http://httpbin.org/get?p=%s" % random.randint(1, 100),
                           method="GET",
                           callback=cb,
                           cookie={1: 21, "dfda": 123, },
                           )
        tasks.append(task)
    pool = EventHttp()
    failed = pool.do_until_done(tasks)
