#  MIT License
#
#  Copyright (c) 2020 Sam McCormack
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
import sys
import time
from multiprocessing import Process, Queue
from typing import List, Tuple

import multiprocess

from scheduler.ProcessTask import ProcessTask
from scheduler.utils import TaskFailedException
from test.utils import (
    _get_input_output_numpy,
    _func_numpy,
    assert_results_numpy,
    _get_input_output,
    _funcq,
    assert_results,
    _func,
    _func_no_params,
    _get_input_output_single_result,
    _func_returns_single_value,
    _get_input_output_two_results,
    _func_returns_two_values,
    _func_no_return,
    _long_task,
    _func_raise_exception,
    _func_print,
)


def test_shared_memory_numpy(scheduler):
    """
    Tests whether shared memory works correctly.
    """
    # scheduler = Scheduler(shared_memory=True, shared_memory_threshold=0)

    args, expected = _get_input_output_numpy()
    for a in args:
        scheduler.add(target=_func_numpy, args=a)

    loop = asyncio.get_event_loop()
    results: List = loop.run_until_complete(scheduler.run())
    assert_results_numpy(expected, results)

    assert scheduler.finished


def test_add_process(scheduler):
    """Tests whether tasks are added to the scheduler correctly with `add_process()`."""
    # scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        q = Queue()
        p = Process(target=_funcq, args=(q,) + a)

        scheduler.add_process(p, q)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_add(scheduler):
    """Tests whether `add()` works correctly."""
    # scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        scheduler.add(target=_func, args=a)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_run_no_params(scheduler):
    """Tests whether `run()` works correctly on a function with no parameters."""
    # scheduler = Scheduler()

    expected = _func_no_params()
    for a in range(50):
        scheduler.add(target=_func_no_params)

    loop = asyncio.get_event_loop()
    results: List = loop.run_until_complete(scheduler.run())

    for r in results:
        assert r == expected

    assert scheduler.finished


def test_multiprocess(scheduler):
    """Tests whether `add()` and `run_blocking()` work correctly with `multiprocess` instead of `multiprocessing`."""
    # scheduler = Scheduler()

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


def test_run_blocking(scheduler):
    """Tests whether `run_blocking()` works correctly."""
    # scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        q = Queue()
        p = Process(target=_funcq, args=(q,) + a)
        scheduler.add_process(p, q)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_run(scheduler):
    """Tests whether `run()` works correctly."""
    # scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        scheduler.add(target=_func, args=a)

    loop = asyncio.get_event_loop()
    results: List[Tuple[int, int, int]] = loop.run_until_complete(scheduler.run())

    assert_results(expected, results)

    assert scheduler.finished


def test_map(scheduler):
    """Tests whether `map()` works correctly."""
    # scheduler = Scheduler()

    args, expected = _get_input_output()

    loop = asyncio.get_event_loop()
    results: List[Tuple[int, int, int]] = loop.run_until_complete(
        scheduler.map(target=_func, args=args)
    )

    assert_results(expected, results)
    assert scheduler.finished


def test_map_one_return_value(scheduler):
    """Tests whether `map()` works correctly when the target function only returns a single result."""
    # scheduler = Scheduler()

    args, expected = _get_input_output_single_result()

    loop = asyncio.get_event_loop()
    results: List[Tuple[int]] = loop.run_until_complete(
        scheduler.map(target=_func_returns_single_value, args=args)
    )

    assert len(expected) == len(results)
    assert all([not hasattr(r, "__len__") for r in results])

    assert scheduler.finished


def test_map_two_return_values(scheduler):
    """Tests whether `map()` works correctly when the target function returns two results."""
    # scheduler = Scheduler()

    args, expected = _get_input_output_two_results()

    loop = asyncio.get_event_loop()
    results: List[Tuple[int, int]] = loop.run_until_complete(
        scheduler.map(target=_func_returns_two_values, args=args)
    )

    assert all([hasattr(r, "__len__") for r in results])
    assert_results(expected, results)
    assert scheduler.finished


def test_map_no_args(scheduler):
    """
    Tests whether `map()` works correctly with no arguments.
    """
    # scheduler = Scheduler()

    loop = asyncio.get_event_loop()
    results: List = loop.run_until_complete(scheduler.map(target=_func_no_params))

    expected = _func_no_params()

    assert all([r == expected for r in results])
    assert scheduler.finished


def test_map_no_return(scheduler):
    """
    Tests whether 'map()' works correctly when the target function
    does not return any values.
    """
    # scheduler = Scheduler()

    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(scheduler.map(target=_func_no_return))

    assert all([r is None for r in results])
    assert scheduler.finished


def test_map_blocking(scheduler):
    """Tests whether `map_blocking()` works correctly."""
    # scheduler = Scheduler()

    args, expected = _get_input_output()

    results: List[Tuple[int, int, int]] = scheduler.map_blocking(
        target=_func, args=args
    )

    assert_results(expected, results)
    assert scheduler.finished


def test_add_task(scheduler):
    """Tests whether tasks are added to the scheduler correctly with `add_task()`."""
    # scheduler = Scheduler()

    args, expected = _get_input_output()
    for a in args:
        q = Queue()
        p = Process(target=_funcq, args=(q,) + a)

        task = ProcessTask(p, q)
        scheduler.add_task(task)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_add_tasks(scheduler):
    """Tests whether tasks are added to the scheduler correctly with `add_tasks()`."""
    # scheduler = Scheduler()
    tasks = []

    args, expected = _get_input_output()
    for a in args:
        q = Queue()
        p = Process(target=_funcq, args=(q,) + a)

        task = ProcessTask(p, q)
        tasks.append(task)

    scheduler.add_tasks(*tasks)

    results: List[Tuple[int, int, int]] = scheduler.run_blocking()
    assert_results(expected, results)

    assert scheduler.finished


def test_terminate(scheduler):
    """Tests whether the scheduler returns the correct result - an empty list - when terminated."""
    # scheduler = Scheduler()
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


def test_raise_exception(scheduler):
    """
    Tests whether the Scheduler responds correctly to a task raising an exception.
    """
    args, expected = _get_input_output()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(scheduler.map(target=_func_raise_exception, args=args))
        assert False, "Scheduler did not raise Exception."
    except Exception as e:
        assert isinstance(e, TaskFailedException)

    assert scheduler.failed


text = ""


def test_stdout(scheduler):
    global text

    expected = "\n".join([f"{i}" for i in range(1000)]) + "\n"

    def temp(_stdout):
        global text
        text += _stdout

    write = sys.stdout.write
    sys.stdout.write = temp

    scheduler.map_blocking(target=_func_print, args=[(expected,),])

    while not text:
        time.sleep(0.01)

    sys.stdout.write = write
    assert text == expected, "Text from stdout is incorrect."
