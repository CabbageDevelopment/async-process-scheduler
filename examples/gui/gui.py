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

"""
Demonstrates the usage of Scheduler in a simple PyQt program, which shows progress and output.

Dependencies are in `requirements.txt`. To install them, run `pip install -r requirements.txt --user`.
"""

import asyncio
import random
import sys
import time
from typing import List, Tuple

import asyncqt
from PyQt5 import QtGui
from PyQt5.QtWidgets import (
    QApplication,
    QLabel,
    QProgressBar,
    QVBoxLayout,
    QWidget,
    QPushButton,
)

from scheduler.Scheduler import Scheduler


def long_calculation(sleep_time: int) -> Tuple[int]:
    """
    Will be executed in another process. Simulates a long calculation,
    and puts the 'result' in the queue.
    """
    time.sleep(sleep_time)
    return (sleep_time,)


class Window(QWidget):
    """Basic Window class."""

    def __init__(self):
        super().__init__()

        layout = QVBoxLayout()
        self.setLayout(layout)

        self.label = QLabel("Output from processes will be shown here", self)
        layout.addWidget(self.label)

        self.progress = QProgressBar(self)
        layout.addWidget(self.progress)

        self.button = QPushButton("Start", self)
        self.button.clicked.connect(self.on_click)
        layout.addWidget(self.button)

        self.scheduler: Scheduler = None

    def on_click(self):
        """Called when the button is clicked."""
        if self.scheduler is None or not self.scheduler.is_running():
            # "Start" was clicked. Start the coroutine which runs the scheduler.
            asyncio.ensure_future(self.do_calculations())
            self.button.setText("Cancel")
        else:
            # "Cancel" was clicked. Terminate the scheduler.
            self.scheduler.terminate()
            self.button.setText("Start")
            self.progress.setValue(0)

    async def do_calculations(self):
        """Does the calculations using a scheduler, and shows the output in the label."""
        self.scheduler = Scheduler(progress_callback=self.on_progress)

        num_processes = 16
        for i in range(num_processes):
            # Generate random input for function.
            sleep = random.randint(1, 8)
            self.scheduler.add(target=long_calculation, args=(sleep,))

        # Run all processes and `await` the results: an ordered list containing one tuple from each process.
        output: List[Tuple] = await self.scheduler.run()

        # If the scheduler was terminated before completion, we don't want the results.
        if not self.scheduler.terminated:
            text = ", ".join([str(i[0]) for i in output])
            self.label.setText(f"Output: {text}")
            self.button.setText("Start")

    def on_progress(self, done: int, total: int) -> None:
        """Updates the progress bar when scheduler finishes a task."""
        if done == 0:
            self.progress.setMaximum(total)
        self.progress.setValue(done)

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """Terminates the scheduler when the window exits."""
        if self.scheduler:
            self.scheduler.terminate()


if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Important: set the event loop using `asyncqt`.
    loop = asyncqt.QEventLoop(app)
    asyncio.set_event_loop(loop)

    window = Window()
    window.show()

    app.exec()
