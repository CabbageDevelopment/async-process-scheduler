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
import sys
from multiprocessing import cpu_count, Process, Queue
from typing import List, Callable, Type, Optional, Union, Any, Tuple, Iterable

import psutil

from scheduler.Task import Task
from scheduler.utils import SchedulerException


class Scheduler:
    """
    A class which handles scheduling tasks, according to the number of CPU cores.

    As an example, a 4-core, 8-thread machine will run 9 processes
    concurrently; an 8-core, 16-thread CPU will run 17 processes
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
        shared_memory: bool = False,
        shared_memory_threshold:int = 1e7,
    ):
        """
        :param progress_callback: a function taking the number of finished tasks and the total number of tasks, which is
        called when a task finishes
        :param update_interval: the time between consecutive updates (when tasks are checked and new tasks are scheduled)
        :param dynamic: whether to dynamically increase the number of processes based on the CPU usage
        :param cpu_threshold: the minimum target CPU usage, in percent; if `dynamic` is enabled and CPU usage is found
        to be below the threshold, the number of simultaneous tasks will be increased
        :param cpu_update_interval: the time, in seconds, between consecutive CPU usage checks when `dynamic` is enabled
        :param shared_memory: whether to use shared memory if possible
        :param shared_memory_threshold: the minimum size of a Numpy array which will cause it to be transferred using shared memory if possible
        """
        self.dynamic = dynamic
        self.update_interval = update_interval

        # Shared memory is only available on Python >= 3.8.
        self.shared_memory = shared_memory and sys.version_info >= (3, 8,)

        if self.shared_memory:
            from multiprocessing.managers import SharedMemoryManager

            self.mgr: SharedMemoryManager = SharedMemoryManager()
        else:
            self.mgr = None

        self.shared_memory_threshold = shared_memory_threshold

        self.tasks: List[Task] = []
        self.output: List[Tuple] = []

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
        args: Tuple = (),
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

        _args = (queue, self.mgr, self.shared_memory_threshold) + args
        _wrapper = functools.partial(wrapper, target)

        process = process_type(target=_wrapper, args=_args)
        self.tasks.append(Task(process, queue, subtasks=subtasks))

    async def map(
        self,
        target: Callable,
        args: Iterable = (),
        subtasks: int = 0,
        process_type: Type = multiprocessing.Process,
        queue_type: Type = multiprocessing.Queue,
    ) -> Union[List[Tuple], List[Any]]:
        """
        Maps arguments over a single function. Each item in 'args' will be used 
        as the input for a single process.

        This is equivalent to adding one or more tasks to the scheduler 
        with 'add()', then starting the scheduler.  

        :param target: the function to run in another process
        :param args: the arguments for the function as a tuple
        :param subtasks: the number of subtasks
        :param process_type: the type of process; you can specify another type such as `multiprocess.Process`
        :param queue_type: the type of queue; you can specify another type such as `multiprocess.Queue`
        :returns an ordered list containing the output of each task if complete, or an empty list if terminated
        """
        for _args in args:
            self.add(
                target=target,
                args=_args,
                subtasks=subtasks,
                process_type=process_type,
                queue_type=queue_type,
            )

        return await self.run()

    def map_blocking(
        self,
        target: Callable,
        args: Iterable = (),
        subtasks: int = 0,
        process_type: Type = multiprocessing.Process,
        queue_type: Type = multiprocessing.Queue,
    ) -> Union[List[Tuple], List[Any]]:
        """
        Equivalent to 'map()', but blocking. 
        
        Maps arguments over a single function. Each item in 'args' will be used 
        as the input for a single process.

        :returns an ordered list containing the output of each task if complete, or an empty list if terminated
        """
        for _args in args:
            self.add(
                target=target,
                args=_args,
                subtasks=subtasks,
                process_type=process_type,
                queue_type=queue_type,
            )

        return self.run_blocking()

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

    async def run(self) -> Union[List[Tuple], List[Any]]:
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

        self._sanitise_output()
        self.finished = True

        self._shutdown()

        return self.output

    def run_blocking(self) -> Union[List[Tuple], List[Any]]:
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

        self._sanitise_output()
        self.finished = True

        self._shutdown()

        return self.output

    def terminate(self) -> None:
        """Terminates all running tasks by killing their processes."""
        if not self.terminated:
            [t.terminate() for t in self.tasks]
            self.terminated = True

        self._shutdown()

    def _shutdown(self) -> None:
        """
        Called to shut down any open resources, such as the shared memory manager.
        """
        if self.mgr:
            self.mgr.shutdown()

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

    def _sanitise_output(self) -> None:
        """
        Converts the output into its expected form. This involves replacing 
        SharedMemoryObject instances with their data.
        """
        for i in range(len(self.output)):
            item = self.output[i]
            if not isinstance(item, tuple):
                if isinstance(item, SharedMemoryObject):
                    self.output[i] = item.get()

                continue

            out = []
            for obj in item:
                if isinstance(obj, SharedMemoryObject):
                    obj = obj.get()

                out.append(obj)

            self.output[i] = tuple(out)

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

        if self.mgr:
            self.mgr.start()

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


def wrapper(
    function: Callable,
    queue: Queue,
    manager: Optional["SharedMemoryManager"],
    threshold: int,
    *args: Any
) -> None:
    """
    Wrapper which calls a function with its specified arguments and puts the output in a queue.

    :param function: the function which will be executed
    :param queue: a Queue object which may be used to transfer data between processes
    :param manager: a SharedMemoryManager or None; used to handle shared memory between processes
    """
    result = function(*args)
    out = []

    if not isinstance(result, tuple):
        result = (result,)  # Convert to tuple.

    if manager:
        for item in result:
            out.append(SharedMemoryObject.attach(manager, item, threshold))
    else:
        out = result

    # If one result was returned from task, switch to single variable instead of tuple.
    if len(out) == 1:
        out = out[0]
    else:
        out = tuple(out)

    queue.put(out)


class DummyNdarray:
    """
    Does nothing. Used when numpy is not installed.
    """


class SharedMemoryObject:
    """
    Wrapper class around a shared memory segment.
    """

    def __init__(self, name: str, shape, dtype):
        self.name = name
        self.shape = shape
        self.dtype = dtype

    @staticmethod
    def attach(
        manager: "SharedMemoryManager", obj: Any, threshold: int
    ) -> Union["SharedMemoryObject", Any]:
        if SharedMemoryObject.is_ndarray(obj) and obj.size > threshold:
            from numpy import ndarray

            arr = obj

            shared = manager.SharedMemory(size=arr.nbytes)
            sm_arr = ndarray(arr.shape, dtype=arr.dtype, buffer=shared.buf)

            sm_arr[:] = arr[:]
            name = shared.name
            shared.close()

            return SharedMemoryObject(name, sm_arr.shape, sm_arr.dtype)

        return obj

    @staticmethod
    def is_ndarray(obj):
        try:
            from numpy import ndarray
        except:
            # Not installed, use dummy type.
            ndarray = DummyNdarray

        return isinstance(obj, ndarray)

    def get(self):
        """
        Gets the value from shared memory, and deletes the shared memory block.
        """
        from multiprocessing import shared_memory as sm
        from numpy import ndarray

        shared = sm.SharedMemory(name=self.name)
        sm_arr = ndarray(self.shape, dtype=self.dtype, buffer=shared.buf)

        arr = ndarray(sm_arr.shape, dtype=sm_arr.dtype)
        arr[:] = sm_arr[:]

        shared.close()
        shared.unlink()

        return arr
