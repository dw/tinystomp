"""
Ultra-minimal STOMP protocol parser/generator.
"""

from __future__ import absolute_import
import collections
import functools
import re
import socket
import time
import urlparse


class Error(Exception):
    """
    Base for any exception class used by this module.
    """


class ProtocolError(Error):
    """
    Raised when a violation of the protocol is detected.
    """


class Frame(object):
    """
    Represent a parsed STOMP frame.
    """
    #: None, or bytestring message body.
    body = None

    def __init__(self, command):
        #: Bytestring command verb.
        self.command = command
        #: Dict of bytestring headers.
        self.headers = {}

    def __repr__(self):
        bits = ['%s %s' % p for p in self.headers.iteritems()]
        bits.sort()
        headers = '\n    '.join(bits)
        return '<%s %r {\n    %s\n}>' % (
            self.command,
            self.body and (self.body[:20]+'..'),
            headers,
        )


def parse_url(url):
    """
    Given a tcp://host:port/ URL, return a (host, port) tuple.
    """
    parsed = urlparse.urlparse(url)
    assert parsed.scheme == 'tcp'
    host, sep, port = parsed.netloc.partition(':')
    assert sep == ':'
    return host, int(port)


_eol_pat = re.compile('\r?\n')
def iterlines(s, start, stop):
    """
    Yield lines from a string, including the absolute starting offset of the
    next line.
    """
    for m in _eol_pat.finditer(s, start, stop):
        mstart, end = m.span()
        yield end + 1, s[start:mstart]
        start = end


#
# Formatters.
#


def _format(command, body, headers):
    """
    Return a formatted STOMP frame as a bytestring.
    """
    bits = [command, '\n']
    if body:
        bits.extend(('content-length:', str(len(body)), '\n'))

    for key, value in headers.iteritems():
        bits.extend((key.replace('_', '-'), ':', str(value), '\n',))
    bits.extend((
        '\n',
        body or '',
        '\x00\n\n\n\n'
    ))
    return ''.join(bits)


def connect(host, **headers):
    """
    Generate a CONNECT frame.
    """
    headers['accept_version'] = '1.0,1.1,1.2'
    headers['host'] = host
    return _format('CONNECT', '', headers)


def send(destination, body=None, **headers):
    """
    Generate a SEND frame.
    """
    headers['destination'] = destination
    return _format('SEND', body, headers)


def subscribe(destination, **headers):
    """
    Generate a SUBSCRIBE frame.
    """
    headers['destination'] = destination
    headers.setdefault('id', str(time.time()))
    return _format('SUBSCRIBE', '', headers)


def unsubscribe(destination, id_, **headers):
    """
    Generate a UNSUBSCRIBE frame.
    """
    headers['id'] = id_
    return _format('UNSUBSCRIBE', '', headers)


def ack(id_, **headers):
    """
    Generate a ACK frame.
    """
    headers['id'] = id_
    return _format('ACK', '', headers)


def nack(id_, **headers):
    """
    Generate a NACK frame.
    """
    headers['id'] = id_
    return _format('NACK', '', headers)


def begin(transaction, **headers):
    """
    Generate a BEGIN frame.
    """
    headers['transaction'] = transaction
    return _format('BEGIN', '', headers)


def commit(transaction, **headers):
    """
    Generate a COMMIT frame.
    """
    headers['transaction'] = transaction
    return _format('COMMIT', '', headers)


def abort(transaction, **headers):
    """
    Generate a ABORT frame.
    """
    headers['transaction'] = transaction
    return _format('ABORT', '', headers)


def disconnect(receipt, **headers):
    """
    Generate a DISCONNECT frame.
    """
    headers['receipt'] = receipt
    return _format('DISCONNECT', '', headers)


#
# Parser.
#


class Parser(object):
    def __init__(self):
        self.s = ''
        self.frames = collections.deque()
        self.frame_eof = None

    def receive(self, s):
        self.s += s
        while self._try_parse():
            pass

    def can_read(self):
        return len(self.frames) != 0

    def next(self):
        return self.frames.popleft()

    def _try_parse(self):
        nul_pos = self.s.find('\x00')
        if nul_pos == -1 or (self.frame_eof and len(self.s) < self.frame_eof):
            return

        it = iterlines(self.s, 0, nul_pos)
        try:
            command = ''
            while command == '':
                _, command = next(it)

            frame = Frame(command)
            while True:
                end, line = next(it)
                if not line:
                    break

                key, sep, value = line.partition(':')
                if not sep:
                    raise ProtocolError('header without colon')
                frame.headers[key] = value
        except StopIteration:
            # Command or end-of-header separator missing.
            return False

        clength = int(frame.headers.get('content-length', '0'))
        if clength == 0 or (end+clength) == nul_pos:
            frame.body = self.s[end:nul_pos]
            self.s = self.s[nul_pos+1:]
            self.frame_eof = None
            self.frames.append(frame)
            return True

        self.frame_eof = end + clength
        return False


class Client(object):
    def __init__(self, host=None, port=None, login=None, passcode=None):
        self.host = host
        self.port = port
        self.login = login
        self.passcode = passcode
        self.parser = Parser()

    @classmethod
    def from_url(cls, url, **kwargs):
        host, port = parse_url(url)
        return cls(host, port, **kwargs)

    def connect(self):
        self.s = socket.socket()
        self.s.connect((self.host, self.port))
        extra = {}
        if self.login or self.passcode:
            extra = {'login': self.login or '',
                     'passcode': self.passcode or ''}
        self.s.send(connect(self.host, **extra))

    def next(self):
        while not self.parser.can_read():
            s = self.s.recv(4096)
            if not s:
                raise ProtocolError('disconnected')
            self.parser.receive(s)
        return self.parser.next()

    def __getattr__(self, k):
        formatter = globals().get(k)

        @functools.wraps(formatter)
        def wrapper(*args, **kwargs):
            pkt = formatter(*args, **kwargs)
            self.s.send(pkt)
        return wrapper
