
# tinystomp

This is an ultra-minimal Stomp message generator and parser for Python. Why
`tinystomp`? Because its parser is around an order of magnitude faster than <a
href="https://github.com/nikipore/stompest/">Stompest</a>, which is a more
thoroughly developed module you should probably try first.

However if you're planning on parsing more than 2500 messages/sec and hope to
have at least half the CPU left for useful work, tinystomp may be a better
option as half a CPU is closer to 30000 messages/sec.

Unlike stompest, tinystop lacks:

* Thorough conformance testing
* Protocol machinery for common frameworks
* Testing with anything except ActiveMQ
* Anything except functions to generate STOMP-formatted bytestrings, a class to
  incrementally parse bytestrings into frames, and a dumb-as-chips synchronous
  debug client connection class

At some point tinystomp will include a Twisted protocol implementation, but
currently that protocol is tightly bound to the project it was written for.
Alternatively it may be turned into a pull request for Stompest.


## Parsing

Message (repeated 50x):

    ['CONNECT\npasscode:123\nlogin:123\naccept-version:1.0,1.1,1.2\nhost:localhost\n\n\x00\n\n\n\n']

stompest:

    100 loops, best of 3: 8.94 msec per loop

tinystomp:

    1000 loops, best of 3: 814 usec per loop


## Generation

Message:

    ['CONNECT\npasscode:123\nlogin:123\naccept-version:1.0,1.1,1.2\nhost:localhost\n\n\x00\n\n\n\n']

stompest:

    10000 loops, best of 3: 59.4 usec per loop

tinystomp:

    100000 loops, best of 3: 6.71 usec per loop
