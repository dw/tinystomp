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

import collections
import unittest
import mock

import tinystomp


class ErrorTest(unittest.TestCase):
    def test_constructor(self):
        tinystomp.Error()


class ProtocolErrorTest(unittest.TestCase):
    def test_constructor(self):
        tinystomp.ProtocolError()


class FrameTest(unittest.TestCase):
    def test_constructor(self):
        f = tinystomp.Frame('cmd')
        assert 'cmd' == f.command
        assert {} == f.headers

    def test_repr(self):
        f = tinystomp.Frame('cmd')
        assert "<cmd None {\n    \n}>" == repr(f)

    def test_repr_headers(self):
        f = tinystomp.Frame('cmd')
        f.headers['a'] = 'b'
        assert "<cmd None {\n    a b\n}>" == repr(f)


class ParseUrlTest(unittest.TestCase):
    def test_parse_url(self):
        h, p = tinystomp.parse_url('tcp://host:1234/')
        assert h == 'host'
        assert p == 1234


class SplitFrameTest(unittest.TestCase):
    def func(self, *args, **kwargs):
        end, it = tinystomp.split_frame(*args, **kwargs)
        return end, list(it)

    def test_empty(self):
        assert (0, []) == self.func('', 0, 0)

    def test_oob(self):
        assert (0, []) == self.func('dave\n', 5, 0)

    def test_offset(self):
        assert (6, ['ave']) == self.func('dave\n\n', 1, 6)

    def test_several(self):
        s = '\ndave\ndave\n\n'
        assert self.func(s, 0, len(s)) == (12, [
            '',
            'dave',
            'dave',
        ])

    def test_prefix_eol_pairs_odd(self):
        s = '\n\r\n\r\n\n\r\ndave\n\n'
        assert self.func(s, 0, len(s)) == (14, ['', 'dave'])

    def test_prefix_eol_pairs_even(self):
        s = '\n\n\r\n\r\n\n\r\ndave\n\n'
        assert self.func(s, 0, len(s)) == (15, ['dave'])


class FormatTest(unittest.TestCase):
    def test_nobody_noheaders(self):
        s = tinystomp._format('cmd', None, {})
        assert 'cmd\n\n\x00' == s

    def test_nobody_headers(self):
        s = tinystomp._format('cmd', None, {
            'a': 'b',
        })
        assert 'cmd\na:b\n\n\x00' == s

    def test_body_headers(self):
        s = tinystomp._format('cmd', 'dave', {
            'a': 'b',
        })
        assert s == (
            'cmd\n'
            'content-length:4\n'
            'a:b\n'
            '\n'
            'dave'
            '\x00'
        )

    def assertParse(self, s, cmd, body, headers):
        p = tinystomp.Parser()
        p.receive(s)
        f = p.next()
        assert f.command == cmd
        assert f.body == body
        assert sorted(f.headers.items()) == sorted(headers.items())

    def test_connect(self):
        s = tinystomp.connect('localhost', a='b')
        self.assertParse(s, 'CONNECT', '', {
            'a': 'b',
            'accept-version': '1.0,1.1,1.2',
            'host': 'localhost'
        })

    def test_send_nobody(self):
        s = tinystomp.send('/foo/bar', a='b')
        self.assertParse(s, 'SEND', '', {
            'a': 'b',
            'destination': '/foo/bar'
        })

    def test_send_body(self):
        s = tinystomp.send('/foo/bar', 'dave', a='b')
        self.assertParse(s, 'SEND', 'dave', {
            'a': 'b',
            'content-length': '4',
            'destination': '/foo/bar'
        })

    def test_subscribe(self):
        s = tinystomp.subscribe('/foo/bar', id=123, a='b')
        self.assertParse(s, 'SUBSCRIBE', '', {
            'a': 'b',
            'destination': '/foo/bar',
            'id': '123',
        })

    def test_unsubscribe(self):
        s = tinystomp.unsubscribe('/foo/bar', 123, a='b')
        self.assertParse(s, 'UNSUBSCRIBE', '', {
            'a': 'b',
            'destination': '/foo/bar',
            'id': '123',
        })

    def test_ack(self):
        s = tinystomp.ack('123', a='b')
        self.assertParse(s, 'ACK', '', {
            'a': 'b',
            'id': '123',
        })

    def test_nack(self):
        s = tinystomp.nack('123', a='b')
        self.assertParse(s, 'NACK', '', {
            'a': 'b',
            'id': '123',
        })

    def test_begin(self):
        s = tinystomp.begin('123', a='b')
        self.assertParse(s, 'BEGIN', '', {
            'a': 'b',
            'transaction': '123',
        })

    def test_commit(self):
        s = tinystomp.commit('123', a='b')
        self.assertParse(s, 'COMMIT', '', {
            'a': 'b',
            'transaction': '123',
        })

    def test_abort(self):
        s = tinystomp.abort('123', a='b')
        self.assertParse(s, 'ABORT', '', {
            'a': 'b',
            'transaction': '123',
        })

    def test_disconnect(self):
        s = tinystomp.disconnect('123', a='b')
        self.assertParse(s, 'DISCONNECT', '', {
            'a': 'b',
            'receipt': '123',
        })


class ParserTest(unittest.TestCase):
    def test_constructor(self):
        p = tinystomp.Parser()
        assert p.s == ''
        assert p.frames == collections.deque()
        assert p.frame_eof is None


class ParserComplianceTest(unittest.TestCase):
    def test_dup_headers_preserve_first(self):
        # STOMP 1.2 "Repeated Header Entries" requires only the first header is
        # preserved.
        p = tinystomp.Parser()
        p.receive('SEND\r\n'
                  'key:value1\r\n'
                  'key:value2\r\n'
                  '\r\n'
                  '\x00')

        f = p.next()
        assert f.headers['key'] == 'value1'


class ParserReceiveTest(unittest.TestCase):
    def test_empty_str(self):
        p = tinystomp.Parser()
        p.receive('')
        assert not p.can_read()

    def test_one_nobody(self):
        p = tinystomp.Parser()
        p.receive(tinystomp.connect('host'))
        assert p.can_read()
        f = p.next()
        assert f.command == 'CONNECT'
        assert f.body == ''
        assert f.headers == {
            'accept-version': '1.0,1.1,1.2',
            'host': 'host'
        }

    def test_one_body(self):
        p = tinystomp.Parser()
        p.receive(tinystomp.send('/foo/bar', 'dave', a='b'))
        assert p.can_read()
        f = p.next()
        assert f.command == 'SEND'
        assert f.body == 'dave'
        assert f.headers == {
            'a': 'b',
            'content-length': '4',
            'destination': '/foo/bar',
        }

    def test_one_ignore_eol(self):
        p = tinystomp.Parser()
        eols = '\n\r\n\n'
        p.receive(eols + tinystomp.send('/foo/bar', 'dave', a='b') + eols)
        assert p.can_read()
        f = p.next()
        assert f.command == 'SEND'
        assert f.body == 'dave'
        assert f.headers == {
            'a': 'b',
            'content-length': '4',
            'destination': '/foo/bar',
        }
        assert not p.can_read()

    def test_two_nobody(self):
        p = tinystomp.Parser()
        p.receive(tinystomp.connect('host') * 2)
        for x in range(2):
            assert p.can_read()
            f = p.next()
            assert f.command == 'CONNECT'
            assert f.body == ''
            assert f.headers == {
                'accept-version': '1.0,1.1,1.2',
                'host': 'host'
            }
        assert not p.can_read()

    def test_two_body(self):
        p = tinystomp.Parser()
        p.receive(tinystomp.send('/foo/bar', 'dave', a='b') * 2)
        for x in range(2):
            assert p.can_read()
            f = p.next()
            assert f.command == 'SEND'
            assert f.body == 'dave'
            assert f.headers == {
                'a': 'b',
                'content-length': '4',
                'destination': '/foo/bar',
            }

    def test_two_ignore_eol(self):
        p = tinystomp.Parser()
        eols = '\n\r\n\n'
        p.receive((eols+tinystomp.send('/foo/bar', 'dave', a='b')+eols) * 2)
        for x in range(2):
            assert p.can_read()
            f = p.next()
            assert f.command == 'SEND'
            assert f.body == 'dave'
            assert f.headers == {
                'a': 'b',
                'content-length': '4',
                'destination': '/foo/bar',
            }
        assert not p.can_read()

    def test_one_partial_inverb(self):
        p = tinystomp.Parser()
        eols = '\n\r\n\n'
        s = eols + tinystomp.send('/foo/bar', 'dave', a='b') + eols
        p.receive(s[:6])
        assert not p.can_read()
        p.receive(s[6:])
        assert p.can_read()

        f = p.next()
        assert f.command == 'SEND'
        assert f.body == 'dave'
        assert f.headers == {
            'a': 'b',
            'content-length': '4',
            'destination': '/foo/bar',
        }

    def test_one_partial_ineols(self):
        p = tinystomp.Parser()
        eols = '\n\r\n\n'
        s = eols + tinystomp.send('/foo/bar', 'dave', a='b') + eols
        p.receive(s[:3])
        assert not p.can_read()
        p.receive(s[3:])
        assert p.can_read()

        f = p.next()
        assert f.command == 'SEND'
        assert f.body == 'dave'
        assert f.headers == {
            'a': 'b',
            'content-length': '4',
            'destination': '/foo/bar',
        }

    def test_one_partial_inheader(self):
        p = tinystomp.Parser()
        eols = '\n\r\n\n'
        s = eols + tinystomp.send('/foo/bar', 'dave', a='b') + eols
        p.receive(s[:12])
        assert not p.can_read()
        p.receive(s[12:])
        assert p.can_read()

        f = p.next()
        assert f.command == 'SEND'
        assert f.body == 'dave'
        assert f.headers == {
            'a': 'b',
            'content-length': '4',
            'destination': '/foo/bar',
        }

    def test_one_partial_inbody(self):
        p = tinystomp.Parser()
        eols = '\n\r\n\n'
        s = eols + tinystomp.send('/foo/bar', 'dave'*2000, a='b') + eols

        p.receive(s[:150])
        assert not p.can_read()
        p.receive(s[150:])
        assert p.can_read()

        f = p.next()
        assert f.command == 'SEND'
        assert f.body == 'dave'*2000
        assert f.headers == {
            'a': 'b',
            'content-length': '8000',
            'destination': '/foo/bar',
        }


class ClientTest(unittest.TestCase):
    def test_constructor(self):
        c = tinystomp.Client('host', 1234, 'login', 'passcode')
        assert c.host == 'host'
        assert c.port == 1234
        assert c.login == 'login'
        assert c.passcode == 'passcode'
        assert isinstance(c.parser, tinystomp.Parser)

    def test_from_url(self):
        c = tinystomp.Client.from_url('tcp://host:1234/')
        assert isinstance(c, tinystomp.Client)
        assert c.host == 'host'
        assert c.port == 1234

    @mock.patch('socket.socket')
    def test_connect(self, sock):
        c = tinystomp.Client.from_url('tcp://host:1234/')
        c.connect()
        assert sock.mock_calls == [
            mock.call(),
            mock.call().connect(('host', 1234)),
            mock.call().send(tinystomp.connect('host')),
        ]

    @mock.patch('socket.socket')
    def test_next(self, sock):
        sock.return_value = mock.Mock(
            recv=mock.Mock(side_effect=[tinystomp.connect('host'), '']))

        c = tinystomp.Client.from_url('tcp://host:1234/')
        c.connect()
        f = c.next()
        assert f.command == 'CONNECT'

        self.assertRaises(tinystomp.ProtocolError, c.next)

    def test_getattr_absent(self):
        c = tinystomp.Client.from_url('tcp://host:1234/')
        self.assertRaises(AttributeError, lambda: c.pants)

    @mock.patch('socket.socket')
    def test_getattr_present(self, sock):
        c = tinystomp.Client.from_url('tcp://host:1234/')
        c.connect()
        c.send('/foo/bar/', 'a', a='b')
        assert sock.mock_calls == [
            mock.call(),
            mock.call().connect(('host', 1234)),
            mock.call().send(tinystomp.connect('host')),
            mock.call().send(tinystomp.send('/foo/bar/', 'a', a='b')),
        ]
