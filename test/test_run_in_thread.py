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
from scheduler.Scheduler import Scheduler
from test import impl


def scheduler():
    return Scheduler(run_in_thread=True)


def test_add_process():
    impl.test_add_process(scheduler())


def test_add():
    impl.test_add(scheduler())


def test_run_no_params():
    impl.test_run_no_params(scheduler())


def test_multiprocess():
    impl.test_multiprocess(scheduler())


def test_run_blocking():
    impl.test_run_blocking(scheduler())


def test_run():
    impl.test_run(scheduler())


def test_map():
    impl.test_map(scheduler())


def test_map_one_return_value():
    impl.test_map_one_return_value(scheduler())


def test_map_two_return_values():
    impl.test_map_two_return_values(scheduler())


def test_map_no_args():
    impl.test_map_no_args(scheduler())


def test_map_no_return():
    impl.test_map_no_return(scheduler())


def test_map_blocking():
    impl.test_map_blocking(scheduler())


def test_add_task():
    impl.test_add_task(scheduler())


def test_add_tasks():
    impl.test_add_tasks(scheduler())
