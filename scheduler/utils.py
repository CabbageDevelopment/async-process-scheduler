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
import sys
import time
from multiprocessing import Process

import psutil


class StdOut:
    def __init__(self, queue):
        self.period = 1

        self.last_update = time.time()
        self.text = []

        self.queue = queue

    def write(self, text: str) -> None:
        self.text.append(text)
        self.update()

    def update(self, force: bool = False) -> None:
        if self.text and (force or time.time() - self.last_update > self.period):
            out = "".join(self.text)

            if out.strip():
                self.queue.put(out)

                self.last_update = time.time()
                self.text = []

    def flush(self) -> None:
        return


def terminate_tree(process: Process):
    """
    Terminates a process along with all of its child processes.
    """
    try:
        pid = process.pid
        for child in psutil.Process(pid).children(recursive=True):
            child.terminate()
    except psutil.NoSuchProcess:
        pass
    finally:
        process.terminate()


class SchedulerException(Exception):
    """
    Exception raised when an error occurs with using Scheduler.
    """


class TaskFailedException(Exception):
    """
    Exception raised when a task fails.
    """

    def __init__(self, msg: str):
        asterisks = "*" * 20
        formatted = (
            f"\n\n{asterisks} TASK FAILED - TRACEBACK STARTS {asterisks}\n\n"
            f"{msg}\n\n"
            f"{asterisks} END OF TRACEBACK {asterisks}\n\n"
        )

        super(TaskFailedException, self).__init__(formatted)
