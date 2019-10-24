# Async Process Scheduler

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![PyPI](https://img.shields.io/pypi/v/AsyncProcessScheduler)](https://pypi.org/project/AsyncProcessScheduler)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/AsyncProcessScheduler)

*Current status: beta.*

## Introduction

Async Process Scheduler is a small Python library which provides a simple, GUI-friendly way to efficiently run many processes while avoiding a callback-based data flow. 

Async Process Scheduler is compatible with `multiprocessing` from the standard library, and equivalent implementations such as `multiprocess`.

> Note: To use Async Process Scheduler in a GUI program, you'll need a library which implements a compatible event loop. For example, [asyncqt](https://github.com/gmarull/asyncqt) can be used with PyQt5.

## Installation

To install Async Process Scheduler, use `pip`:

```
pip install AsyncProcessScheduler --user
``` 

## Usage

To use `Scheduler` in Python, import from `scheduler`:

```python
from scheduler.Scheduler import Scheduler
```

After adding processes to a `Scheduler` instance, running the processes and getting their results is simple:

```python
results: List[Tuple] = await Scheduler.run()
```

Detailed examples are in the [examples](/examples) folder, including a [GUI example](/examples/gui/gui.py) using PyQt5. Here is a code snippet which shows the general workflow:

```python
def long_calculation(queue: Queue):
    time.sleep(5) # Simulate a long calculation.
    queue.put((
        "x", "y", "z" # These values will be returned by the scheduler for each process.
    ))


async def run():
    scheduler = Scheduler()
    num_processes = 16

    for i in range(num_processes):
        queue = Queue()
        process = Process(target=long_calculation, args=(queue,))
        
        scheduler.add(process, queue)

    # Run all processes and get an ordered list containing the results from each.
    results: List[Tuple] = await scheduler.run()

    # Do something with the results.
    print(results)

# Start the coroutine.
asyncio.ensure_future(run()) 
```

## Design

This diagram demonstrates the implementation of `Scheduler`.

![Image demonstrating the design of Scheduler.](/docs/images/scheduler.png)