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
from multiprocessing import cpu_count
from typing import List, Callable

import psutil

from scheduler.Task import Task
from scheduler.utils import SchedulerException


class Scheduler:
    """
    A class which handles scheduling tasks, according to the number of CPU cores.

    As an example, a 4-core, 8-thread machine will run 9 processes
    concurrently and an 8-core, 16-thread CPU will run 17 processes
    concurrently.

    If CPU usage is found to be below the threshold, the number of
    simultaneous processes will be increased. CPU usage is checked
    every 5 seconds.
    """

    def __init__(
        self,
        progress_callback: Callable[[int, int], None] = None,
        delay_seconds: float = 0.05,
    ):
        self.tasks: List[Task] = []
        self.output: List[tuple] = []

        # Minimum number of tasks to run concurrently.
        self.min_concurrent_count: int = cpu_count() + 1

        # The actual number of tasks may be adjusted dynamically to improve efficiency.
        self.concurrent_count: int = self.min_concurrent_count

        # List of currently running tasks.
        self.running_tasks: List[Task] = []

        # The delay between each consecutive check for finished tasks.
        self.delay_seconds = delay_seconds
        self.delay_millis = int(delay_seconds * 1000)

        self.time_start: float = 0
        self.started = False
        self.stopped = False
        self.terminated = False

        # Most recent time at which CPU utilisation was checked.
        self.time_cpu_checked: float = 0

        # Number of seconds between CPU utilisation checks.
        self.time_between_cpu_checks: float = 5

        # If the CPU utilisation in percent is below the threshold, more tasks will be run.
        self.cpu_threshold = 95

        self.total_task_count: int = 0
        self.tasks_completed: int = 0

        # Callback which allows the Scheduler to report its progress;
        # the 1st input is the number of completed tasks, while the 2nd is
        # the total number of tasks.
        self.progress_callback: Callable[[int, int], None] = progress_callback

    def add_task(self, task: Task) -> None:
        """
        Adds a task to the Scheduler.
        """
        if self.started:
            raise SchedulerException("Do not add tasks to an running Scheduler.")

        self.tasks.append(task)

    async def run(self) -> List[tuple]:
        """
        Runs the tasks with coroutines. Returns a list containing
        the output of each task, after all tasks are complete.

        Important: the list is not ordered. Each task should return some
        form of identifier in its results; for example, the name of the
        signal.
        """
        # Initialize `self.output` so that it can be indexed into.
        self.output = [() for _ in self.tasks]
        self.start()

        while not self.stopped and not self.all_tasks_finished():
            await asyncio.sleep(self.delay_seconds)
            self.update()

        return self.output

    def update(self):
        should_update_tasks = False

        t = time.time()
        if t - self.time_cpu_checked > self.time_between_cpu_checks:
            self.time_cpu_checked = t
            total_remaining_tasks = sum(
                [t.total_tasks() for t in self.available_tasks()]
            )

            if total_remaining_tasks > self.concurrent_count:
                cpu_usage = psutil.cpu_percent()

                if cpu_usage < self.cpu_threshold:
                    new_count = int(self.concurrent_count * 100 / cpu_usage)

                    if new_count == self.concurrent_count:
                        new_count += 1

                    self.concurrent_count = new_count
                    should_update_tasks = True

        for t in self.running_tasks:
            t.update()
            if t.finished:
                index = self.tasks.index(t)
                self.output[index] = t.queue.get()

                self.on_task_completed(t)
                should_update_tasks = True

        if should_update_tasks:
            self.schedule_tasks()

    def start(self):
        """Starts the scheduler running the assigned tasks."""
        self.started = True
        self.total_task_count = sum([t.total_tasks() for t in self.tasks])

        self.time_start = time.time()
        self.time_cpu_checked = self.time_start
        self.report_progress(0)

        self.schedule_tasks()

    def schedule_tasks(self):
        """Updates the currently running tasks by starting new tasks if necessary."""
        tasks = self.tasks_to_run()
        self.running_tasks.extend(tasks)
        [t.start() for t in tasks]

    def terminate(self):
        """Terminates all running tasks by killing their processes."""
        if not (self.terminated or self.stopped):
            [t.terminate() for t in self.tasks]
            self.terminated = True
            self.stop_timer()

    def stop_timer(self):
        """
        Stops the timer from checking whether tasks have finished.
        This should be called when all tasks have been completed.
        """
        if not self.stopped:
            self.stopped = True

    def on_task_completed(self, task):
        """Called when a task finishes."""
        self.report_progress(task.total_tasks())
        self.running_tasks.remove(task)

    def on_all_tasks_completed(self):
        """Called when all assigned tasks have been completed."""
        self.stop_timer()

    def report_progress(self, tasks_just_finished: int):
        self.tasks_completed += tasks_just_finished
        if self.progress_callback:
            self.progress_callback(self.tasks_completed, self.total_task_count)

    def available_tasks(self) -> List[Task]:
        """Gets all tasks which are available to run."""
        return [t for t in self.tasks if not (t.running or t.finished)]

    def all_tasks_finished(self) -> bool:
        """Returns whether all tasks have been finished."""
        return all([t.finished for t in self.tasks])

    def total_running_tasks(self) -> int:
        """Returns the total number of running tasks, including sub-tasks."""
        running = self.running_tasks
        return sum([t.total_tasks() for t in running])

    def tasks_to_run(self) -> List[Task]:
        """
        Gets the tasks that should be run, based on the core count
        and the current number of running tasks.
        """
        # Number of remaining tasks to run.
        available = self.available_tasks()

        # The total number of tasks (including sub-tasks) for each available task.
        task_counts = [t.total_tasks() for t in available]

        running_count = self.total_running_tasks()

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
