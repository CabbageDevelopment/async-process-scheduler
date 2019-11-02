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
import functools
import multiprocessing
import time
from multiprocessing import cpu_count, Process, Queue
from typing import List, Callable, Type

import psutil

from scheduler.Task import Task
from scheduler.utils import SchedulerException


class Scheduler:
    """
    A class which handles scheduling tasks, according to the number of CPU cores.

    As an example, a 4-core, 8-thread machine will run 9 processes
    concurrently and an 8-core, 16-thread CPU will run 17 processes
    concurrently.

    If `dynamic` is enabled, Scheduler will also check CPU usage and increase
    the number of processes if it is below the threshold.
    """

    def __init__(
        self,
        progress_callback: Callable[[int, int], None] = None,
        update_interval: float = 0.05,
        dynamic: bool = True,
        cpu_threshold: float = 95,
        cpu_update_interval: float = 5,
    ):
        """
        :param progress_callback: a function taking the number of finished tasks and the total number of tasks, which is
        called when a task finishes
        :param update_interval: the time between consecutive updates (when tasks are checked and new tasks are scheduled)
        :param dynamic: whether to dynamically increase the number of processes based on the CPU usage
        :param cpu_threshold: the minimum target CPU usage, in percent; if `dynamic` is enabled and CPU usage is found
        to be below the threshold, the number of simultaneous tasks will be increased
        :param cpu_update_interval: the time, in seconds, between consecutive CPU usage checks when `dynamic` is enabled
        """
        self.dynamic = dynamic
        self.update_interval = update_interval

        self.tasks: List[Task] = []
        self.output: List[tuple] = []

        # Minimum number of tasks to run concurrently.
        self.min_concurrent_count: int = self.optimal_process_count()

        # The actual number of tasks may be adjusted dynamically to improve efficiency.
        self.concurrent_count: int = self.min_concurrent_count

        # List of currently running tasks.
        self.running_tasks: List[Task] = []

        self.time_start: float = 0
        self.started = False
        self.finished = False
        self.terminated = False

        # Most recent time at which CPU utilisation was checked.
        self.time_cpu_checked: float = 0

        # Number of seconds between CPU utilisation checks.
        self.time_between_cpu_checks: float = cpu_update_interval

        # If the CPU utilisation in percent is below the threshold, more tasks will be run.
        self.cpu_threshold = cpu_threshold

        self.total_task_count: int = 0
        self.tasks_completed: int = 0

        # Callback which allows the Scheduler to report its progress;
        # the 1st input is the number of completed tasks, and the 2nd is
        # the total number of tasks.
        self.progress_callback: Callable[[int, int], None] = progress_callback

    def add_process(self, process: Process, queue: Queue, subtasks: int = 0) -> None:
        """
        Creates a task from a process and queue, and adds it to the scheduler.
        """
        if self.started:
            raise SchedulerException(
                "add() cannot be called on a Scheduler which has already been started."
            )

        task = Task(process, queue, subtasks)
        self.tasks.append(task)

    def add(
        self,
        target: Callable,
        args: tuple,
        subtasks: int = 0,
        process_type: Type = multiprocessing.Process,
        queue_type: Type = multiprocessing.Queue,
    ) -> None:
        """
        Creates a task from a normal function which directly returns output.

        :param target: the function to run in another process
        :param args: the arguments for the function as a tuple
        :param subtasks: the number of subtasks
        :param process_type: the type of process; you can specify another type such as `multiprocess.Process`
        :param queue_type: the type of queue; you can specify another type such as `multiprocess.Queue`
        """
        if self.started:
            raise SchedulerException(
                "add_function() cannot be called on a scheduler which has already been started."
            )

        queue = queue_type()

        _args = (queue,) + args
        _wrapper = functools.partial(wrapper, target)

        process = process_type(target=_wrapper, args=_args)
        self.tasks.append(Task(process, queue, subtasks=subtasks))

    def add_task(self, task: Task) -> None:
        """
        Adds a task to the Scheduler.
        """
        if self.started:
            raise SchedulerException(
                "add_task() cannot be called on a Scheduler which has already been started."
            )

        self.tasks.append(task)

    def add_tasks(self, *args: Task) -> None:
        """
        Adds multiple tasks to the Scheduler.
        """
        if self.started:
            raise SchedulerException(
                "add_tasks() cannot be called on a Scheduler which has already been started."
            )

        self.tasks.extend(args)

    async def run(self) -> List[tuple]:
        """
        Runs the tasks in a coroutine.

        :returns an ordered list containing the output of each task if complete, or an empty list if terminated
        """
        if self.started:
            raise SchedulerException("Scheduler has already been started.")

        self._initialize_output()
        self._start()

        while not self.terminated and not self._all_tasks_finished():
            await asyncio.sleep(self.update_interval)
            self._update()

        if self.terminated:
            return []

        self.finished = True
        return self.output

    def run_blocking(self) -> List[tuple]:
        """
        Runs the tasks. Will block the current thread until all tasks are complete.

        :returns an ordered list containing the output of each task if complete, or an empty list if terminated
        """
        if self.started:
            raise SchedulerException("Scheduler has already been started.")

        self._initialize_output()
        self._start()

        while not self.terminated and not self._all_tasks_finished():
            time.sleep(self.update_interval)
            self._update()

        if self.terminated:
            return []

        self.finished = True
        return self.output

    def terminate(self) -> None:
        """Terminates all running tasks by killing their processes."""
        if not self.terminated:
            [t.terminate() for t in self.tasks]
            self.terminated = True

    def is_running(self) -> bool:
        """:returns whether the scheduler is running."""
        return self.started and not self.finished and not self.terminated

    @staticmethod
    def optimal_process_count() -> int:
        """
        :returns the optimal number of processes based on the number of logical processors.
        """
        return cpu_count() + 1

    def _initialize_output(self) -> None:
        # Initialize `self.output` so that it can be indexed into.
        self.output = [None for _ in self.tasks]

    def _update(self) -> None:
        """
        Checks whether tasks have finished, and schedules new tasks if applicable.
        """
        schedule_new_tasks = False

        t = time.time()
        if self.dynamic and t - self.time_cpu_checked > self.time_between_cpu_checks:
            self.time_cpu_checked = t
            total_remaining_tasks = sum(
                [t.total_tasks() for t in self._available_tasks()]
            )

            if total_remaining_tasks > self.concurrent_count:
                cpu_usage = psutil.cpu_percent()

                if cpu_usage < self.cpu_threshold:
                    new_count = int(self.concurrent_count * 100 / cpu_usage)

                    if new_count == self.concurrent_count:
                        new_count += 1

                    self.concurrent_count = new_count
                    schedule_new_tasks = True

        for t in self.running_tasks:
            t.update()
            if t.finished:
                index = self.tasks.index(t)
                self.output[index] = t.queue.get()

                self._on_task_completed(t)
                schedule_new_tasks = True

        if schedule_new_tasks:
            self._schedule_tasks()

    def _start(self) -> None:
        """
        Starts the scheduler running its tasks.
        """
        self.started = True
        self.total_task_count = sum([t.total_tasks() for t in self.tasks])

        self.time_start = time.time()
        self.time_cpu_checked = self.time_start
        self._report_progress(0)

        self._schedule_tasks()

    def _schedule_tasks(self) -> None:
        """Updates the currently running tasks by starting new tasks if necessary."""
        tasks = self._tasks_to_run()
        self.running_tasks.extend(tasks)
        [t.start() for t in tasks]

    def _on_task_completed(self, task) -> None:
        """Called when a task finishes."""
        self._report_progress(task.total_tasks())
        self.running_tasks.remove(task)

    def _report_progress(self, tasks_just_finished: int) -> None:
        self.tasks_completed += tasks_just_finished
        if self.progress_callback:
            self.progress_callback(self.tasks_completed, self.total_task_count)

    def _available_tasks(self) -> List[Task]:
        """Gets all tasks which are available to run."""
        return [t for t in self.tasks if not (t.running or t.finished)]

    def _all_tasks_finished(self) -> bool:
        """Returns whether all tasks have been finished."""
        return all([t.finished for t in self.tasks])

    def _total_running_tasks(self) -> int:
        """Returns the total number of running tasks, including sub-tasks."""
        running = self.running_tasks
        return sum([t.total_tasks() for t in running])

    def _tasks_to_run(self) -> List[Task]:
        """
        Gets the tasks that should be run, based on the core count
        and the current number of running tasks.
        """
        # Number of remaining tasks to run.
        available = self._available_tasks()

        # The total number of tasks (including sub-tasks) for each available task.
        task_counts = [t.total_tasks() for t in available]

        running_count = self._total_running_tasks()

        # Number of tasks that can be started without reducing efficiency.
        num_to_run = self.concurrent_count - running_count

        final_task_index = 0
        for i in range(1, len(task_counts) + 1):
            total = sum(task_counts[:i])

            if total <= num_to_run:
                final_task_index = i
            else:
                break

        if final_task_index == 0 and running_count == 0:
            final_task_index += 1

        return available[:final_task_index]


def wrapper(function: Callable, queue: Queue, *args):
    """
    Wrapper which calls a function with its specified arguments and puts the output in a queue.
    """
    result: tuple = function(*args)
    queue.put(result)
