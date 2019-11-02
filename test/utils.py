#  MIT License
#
#  Copyright (c) 2019 Sam McCormack
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

import time
from multiprocessing import Process
from multiprocessing.queues import Queue
from typing import Tuple, List


def _long_task(_time: int = 1):
    """Pretends to be a long task."""
    time.sleep(_time)
    return 1, 2, 3


def _get_process_and_queue(target, *args):
    """Returns a process and a queue for testing."""
    queue = Queue()
    return Process(target=target, args=(queue,) + tuple(args)), queue


def _get_input_output() -> Tuple[List, List]:
    args = [(i, i + 2, i + 5) for i in range(50)]
    expected_output = [(_func(*a)) for a in args]
    return args, expected_output


def assert_results(expected, results):
    assert len(expected) == len(results)

    count = len(results)
    for i in range(len(results)):
        assert expected[i] == results[i]

        if i < count - 1:
            assert expected[i] != results[i + 1]


def _func(x, y, z) -> Tuple:
    return x ** 2, y ** 3, z ** 4


def _funcq(queue, x, y, z) -> None:
    queue.put((x ** 2, y ** 3, z ** 4))
