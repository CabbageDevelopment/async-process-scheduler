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

from multiprocessing import Process, Queue

from scheduler.utils import terminate_tree


class Task:
    """
    A simple class containing a process and an associated queue. The queue should have been
    passed to the process, so that the process can put its output in the queue.
    """

    def __init__(self, process: Process, queue: Queue, subtasks: int = 0):
        self.process = process
        self.queue = queue

        self.running = False
        self.finished = False

        # The number of processes which will be spawned as part of this task.
        self.subtasks: int = subtasks

    def start(self) -> None:
        """Starts the task."""
        if not self.running and not self.finished:
            self.process.start()
            self.running = True

    def terminate(self) -> None:
        """
        Terminates the task and all running sub-tasks.
        """
        try:
            terminate_tree(self.process)
            self.queue.close()
        except:
            pass
        finally:
            self.running = False

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
        if self.running and self.has_result():
            self.finished = True
            self.running = False

    def has_result(self) -> bool:
        """Returns whether the task has finished."""
        return not self.queue.empty()
