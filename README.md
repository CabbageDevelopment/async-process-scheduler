# Async Process Scheduler

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/AsyncProcessScheduler.svg)](https://badge.fury.io/py/AsyncProcessScheduler)

*Current status: BETA. Improved documentation coming soon.*

## Introduction

Async Process Scheduler is a small Python library which provides a simple, GUI-friendly way to efficiently run many processes while avoiding the drawbacks of a callback-based data flow.

Async Process Scheduler is compatible with Python 3.6 and higher.

## Installation

To install Async Process Scheduler, run `pip install AsyncProcessScheduler --user`. 

In python, import using `import scheduler`.

## Usage

After adding processes to a `Scheduler` instance, running the processes and getting their results is as simple as `await`-ing `Scheduler.run()`.

Here is an example which shows the general workflow while simplifying the irrelevant code:

```python
class MyWindow:

    ### [GUI code removed for simplicity.] ###

    def start_scheduler(self):
        # Starts the coroutine.
        asyncio.ensure_future(self.coro_run())

    async def coro_run(self):
        self.scheduler = Scheduler()

        for i in range(10):
            queue = Queue()
            process = Process(target=long_calculation, args=(queue,))

            self.scheduler.add_task(Task(process, queue))

        # Run all processes and get the results without blocking the GUI.
        all_results = await self.scheduler.run()

        ### [Do something with the GUI using the reults.] ###
        ...


def long_calculation(queue):
    ### [Do some long calculations, producing x,y,z.] ###
    queue.put((
        x, y, z # These values will be returned by the scheduler.
    ))
```
