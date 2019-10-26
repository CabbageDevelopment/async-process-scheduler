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
import asyncio
import time
from multiprocessing import Process, Queue
from typing import List, Tuple

from scheduler.Scheduler import Scheduler
from scheduler.Task import Task


def long_task(queue: Queue, _time: int = 1):
    """Pretends to be a long task."""
    time.sleep(_time)
    queue.put((1, 2, 3))


def get_process_and_queue(target, *args):
    """Returns a process and a queue for testing."""
    queue = Queue()
    return Process(target=target, args=(queue,) + tuple(args)), queue


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


def test_terminate():
    """Tests whether the scheduler returns the correct result - an empty list - when terminated."""
    scheduler = Scheduler()
    count = 10

    async def run_scheduler():
        # Create a lot of processes which take 100 seconds each.
        for i in range(count):
            scheduler.add(*get_process_and_queue(long_task, 100))

        # This will take a while. While running, it will be terminated by the other coroutine.
        results: List[Tuple] = await scheduler.run()

        # Check that an empty list is returned when terminated.
        return scheduler.terminated and isinstance(results, list) and len(results) == 0

    async def do_run():
        await asyncio.sleep(1)

        result = await asyncio.gather(run_scheduler(), terminate())
        success = result and result[0]

        return success

    async def terminate():
        await asyncio.sleep(1)
        scheduler.terminate()

    loop = asyncio.get_event_loop()
    success = loop.run_until_complete(do_run())

    assert success
