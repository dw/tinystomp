"""
Ultra-minimal STOMP protocol parser/generator.
"""

# The MIT License (MIT)
# 
# Copyright (c) 2016, David Wilson
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


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


_double_eol_pat = re.compile('\r?\n\r?\n')
def split_frame(s, start, stop):
    """
    Find and split all the command and header lines from the first frame in
    s[start:stop], returning the first offset past the end of the last line and
    an iterable yielding the lines.

    Ignores any pair of consecutive EOLs prefixing s to prevent header parsing
    from failing when vertical whitespace is present (allowed by the protocol).
    """
    for m in _double_eol_pat.finditer(s, start, stop):
        mstart, mend = m.span()
        # If the span starts where we started matching, then a pair of
        # prefixing EOLs have been found. Note the new starting position and
        # continue looping in that case.
        if start != mstart:
            lines = s[start:mstart].splitlines()
            return mend, iter(lines)
        start = mend
    return 0, iter(())


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
        '\x00'
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
    headers['destination'] = destination
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
    """
    Incremental parser for a stream of STOMP formatted messages. Invoke
    :py:meth:`receive` as often as desired when data arrives, then call
    :py:meth:`can_read` afterwards in a loop to discover if there are fully
    parsed messages waiting. If there are, call :py:meth:`next` to consume them
    one at a time.

    ::

        p = tinystomp.Parser()
        p.receive(tinystomp.send('/foo/bar', 'data', a='1'))
        assert p.can_read()
        f = p.next()
        assert f.command == 'SEND'
        assert f.body == 'data'
        assert f.headers = {
            'destination': '/foo/bar',
            'a': '1',
        }
    """
    def __init__(self):
        self.s = ''
        self.frames = collections.deque()
        self.frame_eof = None

    def receive(self, s):
        """
        Consume the bytestring `s`.
        """
        self.s += s
        while self._try_parse():
            pass

    def can_read(self):
        """
        Return ``True`` if there are unconsumed frames waiting to be read using
        :py:meth:`next`.
        """
        return len(self.frames) != 0

    def next(self):
        """
        Consume the next available frame, or raise IndexError if no frame is
        available.
        
        :returns:
            tinystomp.Frame instance.
        """
        return self.frames.popleft()

    def _try_parse(self):
        nul_pos = self.s.find('\x00')
        if nul_pos == -1 or (self.frame_eof and len(self.s) < self.frame_eof):
            return

        end, it = split_frame(self.s, 0, nul_pos)
        try:
            command = next(it)
            if not command:
                # split_frame() guarantees at most one blank prefixing line.
                command = next(it)

            frame = Frame(command)
            setdefault = frame.headers.setdefault

            for line in it:
                key, sep, value = line.partition(':')
                if not sep:
                    raise ProtocolError('header without colon')
                setdefault(key, value)
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
    """
    Dumb synchronous debug client suitable for scripts that perform simple
    actions, like inject or monitor bus messages.

    ::

        c = tinystomp.Client('localhost', login='abc', passcode='def')
        c.connect()
        c.subscribe('/a/b')
        while True:
            print 'received frame:', c.next()

    A magic :py:meth:`__getattr__` exists that forwards method calls with the
    same name as a formatter function defined in the tinystomp module on to
    that function, then sends its result to the server. This way every public
    formatter function name is also a method name of this class.
    """
    def __init__(self, host=None, port=None, login=None, passcode=None):
        self.host = host
        self.port = port
        self.login = login
        self.passcode = passcode
        self.parser = Parser()

    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Construct an instance from a tcp://host:port/ URL.
        """
        host, port = parse_url(url)
        return cls(host, port, **kwargs)

    def connect(self):
        """
        Create our TCP connection and send our CONNECT message.
        """
        self.s = socket.socket()
        self.s.connect((self.host, self.port))
        extra = {}
        if self.login or self.passcode:
            extra = {'login': self.login or '',
                     'passcode': self.passcode or ''}
        self.s.send(connect(self.host, **extra))

    def next(self):
        """
        Block waiting for the next available frame, returning it when received.
        """
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
