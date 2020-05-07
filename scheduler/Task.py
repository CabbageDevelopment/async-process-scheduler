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
from abc import ABC, abstractmethod
from typing import Optional


class Task(ABC):
    """
    A simple class containing a process and an associated queue. The queue should have been
    passed to the process, so that the process can put its output in the queue.
    """

    def __init__(self, queue, exc_queue=None, stdout_queue=None, subtasks: int = 0):
        """
        :param queue: Queue for data to be passed.
        :param exc_queue: Queue for exceptions to be passed.
        :param subtasks: Number of subtasks.
        """
        self.queue = queue
        self.exc_queue = exc_queue

        self.stdout_queue = stdout_queue
        self.stdout_text = None

        self.running = False
        self.finished = False

        self.failed = False
        self.exception_tb = None

        # The number of processes which will be spawned as part of this task.
        self.subtasks: int = subtasks

    @abstractmethod
    def start(self) -> None:
        """Starts the task."""
        pass

    @abstractmethod
    def terminate(self) -> None:
        """
        Terminates the task and all running sub-tasks.
        """
        pass

    def total_tasks(self) -> int:
        """
         Returns the total number of tasks associated with this
         instance, including sub-tasks.
         """
        return 1 + self.subtasks

    def update(self) -> None:
        """
        Checks whether the task has finished, and sets properties
        according to the result.
        """
        if self.has_exception():
            self.exception_tb = self.exc_queue.get()
            self.running = False
            self.failed = True

        if self.running and self.has_result():
            self.finished = True
            self.running = False

    def has_result(self) -> bool:
        """Returns whether the task has finished."""
        return not self.queue.empty()

    def has_exception(self) -> bool:
        """
        Returns whether the task has raised an exception.
        """
        return self.exc_queue and not self.exc_queue.empty()

    def has_stdout(self) -> bool:
        """
        Returns whether the task has provided any `stdout`.
        """
        return self.stdout_queue and not self.stdout_queue.empty()

    def get_stdout(self) -> Optional[str]:
        """
        Returns the `stdout` from the task.
        """
        text = self.stdout_text or ""
        if self.has_stdout():
            lines = self.stdout_queue.get()

            if lines:
                if isinstance(lines, tuple):
                    lines = "\n".join(lines)

                text = f"{text}{lines}"

        if text:
            self.stdout_text = None
            return text

        return None
