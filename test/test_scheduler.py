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
from multiprocessing import Process, Queue

from scheduler.Scheduler import Scheduler
from scheduler.Task import Task


def long_task():
    """Pretends to be a long task."""
    time.sleep(1)


def get_process_and_queue(target, *args):
    """Returns a process and a queue for testing."""
    queue = Queue()
    return Process(target=long_task, args=(queue,) + tuple(args)), queue


def test_add():
    """Tests whether tasks are added to the scheduler correctly with `add()`."""
    scheduler = Scheduler()

    count = 100
    tasks = []

    for i in range(count):
        p, q = get_process_and_queue(long_task)
        scheduler.add(p, q)

        task = Task(p, q)
        tasks.append(task)

    for i in range(len(scheduler.tasks)):
        t = scheduler.tasks[i]

        assert tasks[i].process is t.process
        assert tasks[i].queue is t.queue

        if i < count - 1:
            assert tasks[i + 1].process is not t.process
            assert tasks[i + 1].queue is not t.queue


def test_add_task():
    """Tests whether tasks are added to the scheduler correctly with `add_task()`."""
    scheduler = Scheduler()

    count = 100
    tasks = []

    for i in range(count):
        p, q = get_process_and_queue(long_task)
        task = Task(p, q)
        tasks.append(task)
        scheduler.add_task(task)

    for i in range(len(scheduler.tasks)):
        t = scheduler.tasks[i]
        assert tasks[i] is t
        if i < count - 1:
            assert tasks[i + 1] is not t


def test_add_tasks():
    """Tests whether tasks are added to the scheduler correctly with `add_tasks()`."""
    scheduler = Scheduler()

    count = 100
    tasks = []

    for i in range(count):
        p, q = get_process_and_queue(long_task)
        task = Task(p, q)
        tasks.append(task)

    scheduler.add_tasks(*tasks)

    for i in range(len(scheduler.tasks)):
        t = scheduler.tasks[i]
        assert tasks[i] is t
        if i < count - 1:
            assert tasks[i + 1] is not t
