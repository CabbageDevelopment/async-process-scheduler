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
from multiprocessing import Process, Queue
from typing import List, Tuple

import multiprocess

from scheduler.Scheduler import Scheduler
from scheduler.Task import Task

try:
    from test.utils import (
        _long_task,
        _get_input_output,
        _get_input_output_numpy,
        assert_results,
        assert_results_numpy,
        _func,
        _funcq,
        _func_no_params,
        _func_numpy,
        _func_no_return,
    )
except:
    from utils import (
        _long_task,
        _get_input_output,
        _get_input_output_numpy,
        assert_results,
        assert_results_numpy,
        _func,
        _funcq,
        _func_no_params,
        _func_numpy,
        _func_no_return,
    )


def test_shared_memory_numpy():
    """Tests whether `run_blocking()` works correctly."""
    scheduler = Scheduler(shared_memory=True, shared_memory_threshold=0)

    args, expected = _get_input_output_numpy()
    for a in args:
        scheduler.add(target=_func_numpy, args=a)

    loop = asyncio.get_event_loop()
    results: List = loop.run_until_complete(scheduler.run())
    assert_results_numpy(expected, results)

    assert scheduler.finished


def test_add_process():
    """Tests whether tasks are added to the scheduler correctly with `add()`."""
    scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        q = Queue()
        p = Process(target=_funcq, args=(q,) + a)

        scheduler.add_process(p, q)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_add():
    """Tests whether `add()` works correctly."""
    scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        scheduler.add(target=_func, args=a)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_run_no_params():
    """Tests whether `run()` works correctly on a function with no parameters."""
    scheduler = Scheduler()

    expected = _func_no_params()
    for a in range(50):
        scheduler.add(target=_func_no_params)

    loop = asyncio.get_event_loop()
    results: List = loop.run_until_complete(scheduler.run())

    for r in results:
        assert r == expected

    assert scheduler.finished


def test_multiprocess():
    """Tests whether `add()` works correctly with `multiprocess` instead of `multiprocessing`."""
    scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        scheduler.add(
            target=_func,
            args=a,
            process_type=multiprocess.Process,
            queue_type=multiprocess.Queue,
        )

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_run_blocking():
    """Tests whether `run_blocking()` works correctly."""
    scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        q = Queue()
        p = Process(target=_funcq, args=(q,) + a)
        scheduler.add_process(p, q)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_run():
    """Tests whether `run()` works correctly."""
    scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        scheduler.add(target=_func, args=a)

    loop = asyncio.get_event_loop()
    results: List[Tuple[int, int, int]] = loop.run_until_complete(scheduler.run())

    assert_results(expected, results)

    assert scheduler.finished


def test_map():
    """Tests whether `map()` works correctly."""
    scheduler = Scheduler()

    args, expected = _get_input_output()

    loop = asyncio.get_event_loop()
    results: List[Tuple[int, int, int]] = loop.run_until_complete(
        scheduler.map(target=_func, args=args)
    )

    assert_results(expected, results)
    assert scheduler.finished


def test_map_no_args():
    """
    Tests whether `map()` works correctly with no araguments.
    """
    scheduler = Scheduler()

    loop = asyncio.get_event_loop()
    results: List = loop.run_until_complete(scheduler.map(target=_func_no_params))

    expected = _func_no_params()

    assert all([r == expected for r in results])
    assert scheduler.finished


def test_map_no_return():
    """
    Tests whether 'map()' works correctly when the target function 
    does not return any values.
    """
    scheduler = Scheduler()

    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(scheduler.map(target=_func_no_return))

    assert all([r is None for r in results])
    assert scheduler.finished


def test_map_blocking():
    """Tests whether `map_blocking()` works correctly."""
    scheduler = Scheduler()

    args, expected = _get_input_output()

    loop = asyncio.get_event_loop()
    results: List[Tuple[int, int, int]] = scheduler.map_blocking(
        target=_func, args=args
    )

    assert_results(expected, results)
    assert scheduler.finished


def test_add_task():
    """Tests whether tasks are added to the scheduler correctly with `add_task()`."""
    scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        q = Queue()
        p = Process(target=_funcq, args=(q,) + a)

        task = Task(p, q)
        scheduler.add_task(task)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_add_tasks():
    """Tests whether tasks are added to the scheduler correctly with `add_tasks()`."""
    scheduler = Scheduler()
    tasks = []

    args, expected = _get_input_output()
    for a in args:
        q = Queue()
        p = Process(target=_funcq, args=(q,) + a)

        task = Task(p, q)
        tasks.append(task)

    scheduler.add_tasks(*tasks)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_terminate():
    """Tests whether the scheduler returns the correct result - an empty list - when terminated."""
    scheduler = Scheduler()
    count = 10

    async def run_scheduler():
        # Create a lot of processes which take 100 seconds each.
        for i in range(count):
            scheduler.add(target=_long_task, args=(100,))

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
    assert not scheduler.finished


if __name__ == "__main__":
    test_shared_memory_numpy()
    print("Finished.")
