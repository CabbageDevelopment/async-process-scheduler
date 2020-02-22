# Async Process Scheduler

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![PyPI](https://img.shields.io/pypi/v/AsyncProcessScheduler)](https://pypi.org/project/AsyncProcessScheduler)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/AsyncProcessScheduler)
[![Build Status](https://travis-ci.org/CabbageDevelopment/async-process-scheduler.svg?branch=master)](https://travis-ci.org/CabbageDevelopment/async-process-scheduler)

*Current status: beta. Breaking changes may occur before v1.0.0.*

## Introduction

Async Process Scheduler is a small Python library which provides a simple, GUI-friendly way to efficiently run many processes while avoiding a callback-based data flow.

Async Process Scheduler is compatible with `multiprocessing` from the standard library, and equivalent implementations such as `multiprocess`.

> **Note:** To use Async Process Scheduler in a GUI program, you'll need a library which implements a compatible event loop. For example, [qasync](https://github.com/CabbageDevelopment/qasync) can be used with PyQt5.

## Installation

To install Async Process Scheduler, use `pip`:

```bash
pip install AsyncProcessScheduler
```

## Example

Detailed examples are in the [examples](/examples) folder, including a [GUI example](/examples/gui/gui.py) using PyQt5. Here is a code snippet which shows the general workflow:

```python
from scheduler.Scheduler import Scheduler

def long_calculation(x: int, y: int) -> Tuple[int, int]:
    """Simulates a long calculation and returns two numbers."""
    time.sleep(5)
    return x, y

async def run() -> None:
    """Runs 16 processes with the scheduler and prints the results."""
    scheduler = Scheduler()

    # Create (x, y) inputs for 16 processes.
    num_processes = 16
    args = [(i, i+1) for i in range(num_processes)]

    # Run all processes and get an ordered list containing the results from each.
    results: List[Tuple] = await scheduler.map(target=long_calculation, args=args)

    # Do something with the results.
    print(results)

# Start the coroutine (blocking to prevent the program from exiting).
loop = asyncio.get_event_loop() 
loop.run_until_complete(run())
```

> **Note:** GUI programs would use `asyncio.ensure_future(run())` to start the coroutine without blocking.

## Quick guide

This guide explains the basic usage of Async Process Scheduler.

### Importing Scheduler

To use `Scheduler` in Python, import from `scheduler`:

```python
from scheduler.Scheduler import Scheduler
```

### Creating a scheduler

Scheduler instances can be created with or without a progress callback. The progress callback is a function which takes the number of tasks completed and the total number of tasks. 

```python
def on_progress(finished: int, total: int) -> None:
    print(f"{finished} of {total} tasks are complete.")

# Without progress callback.
scheduler = Scheduler()

# With progress callback.
scheduler = Scheduler(on_progress)

# With progress callback.
scheduler = Scheduler(progress_callback=on_progress)
```

The progress callback is called on the thread which the coroutine runs in, and can be used to modify the GUI.

> :warning: This functionality may change before v1.0.0.

### Mapping using a scheduler

The easiest way to run code using a Scheduler is to map an iterable of inputs over a function. 

This allows you to supply a function, and a list of tuples containing the inputs to the function. The output will be calculated in a separate process for each set of inputs.

#### Mapping in a coroutine

```python
"""
Snippet which demonstrates mapping values over a function.
"""

def my_calculation(x: int, y: float, z: str) -> Tuple[int, float, str]:
    """Simulates a long calculation and returns the function parameters."""
    time.sleep(5)
    return x, y, z

scheduler = Scheduler()
args = [
    (1, 3.14, "test1"),  # Args for first process.
    (0, 2.10, "test2"),  # Args for second process.
    (5, 10.0, "test3"),  # Args for third process.
]
```

The results can then be computed in a coroutine:

```python
results: List = await scheduler.map(target=my_calculation, args=args)
```

#### Mapping (blocking)

```python
results: List = scheduler.map_blocking(target=my_calculation, args=args)
```

### Adding tasks to a scheduler

Instead of using `map()`, tasks can individually be added to a scheduler. After adding all tasks, the scheduler can be started with `run()`.

You can add normal functions to a scheduler. If you're migrating from process-oriented code, you may find it easier to add processes and queues to a scheduler instead.

Every task added to a scheduler will be run as a separate process. For maximum efficiency, you should aim to add a number of tasks greater than the number of logical cores. The optimal number for the current CPU is returned by the static method `Scheduler.optimal_process_count()`.

For simplicity, the examples below only add one task to the scheduler.

#### Adding functions

`add()` can be used to add a function to the scheduler. `add()` is similar to the constructor of `Process`.

```python
"""
Snippet which demonstrates adding functions to a scheduler.
"""

def my_calculation(x: int, y: float, z: str) -> Tuple[int, float, str]:
    """Simulates a long calculation and returns the function parameters."""
    time.sleep(5)
    return x, y, z

scheduler = Scheduler()
args = (1, 3.14, "test",)

# Without named arguments.
scheduler.add(my_calculation, args)

# With named arguments.
scheduler.add(target=my_calculation, args=args)
```

Internally, `add()` creates a process and queue which will be used to run your function and get the results. To use types other than those from `multiprocessing`, you can specify them with the `process_type` and `queue_type` arguments.

```python
"""
Snippet which demonstrates adding a function to scheduler using Processes and Queues
from `multiprocess` instead of `multiprocessing`.
"""
from multiprocess import Process, Queue

scheduler = Scheduler()
scheduler.add(
    target=my_function,
    args=(1,2,3,),
    process_type=Process,
    queue_type=Queue
)
```

#### Adding processes

`add_process()` can be used to add a process and queue to the scheduler.

```python
"""
Snippet which demonstrates adding processes and queues to a scheduler.
"""

def my_calculation(queue: Queue, x: int, y: int) -> None:
    """Function which will be run using a process."""
    time.sleep(5)

    # Important: put results in queue instead of returning them.
    queue.put((
        x, y
    ))

scheduler = Scheduler()

queue = Queue()
process = Process(target=my_calculation, args=(queue, 1, 2))

scheduler.add_process(process, queue)
```

> :warning: When adding processes, ensure that the queue instance passed to the function is the same as the queue added to the scheduler. Also take care that the function puts its output in the queue instead of returning it.

#### Sub-tasks

The functions above take an optional `subtasks` parameter. `subtasks` is used to hint to the scheduler that each process may create its own processes; this will be taken into account when scheduling processes.

For example, if each process creates 4 processes you could use `scheduler.add(target=my_process, args=my_args, subtasks=4)`.

### Running a scheduler

When a scheduler runs, it will run all tasks until complete and then return an ordered list containing the output from each task.

#### Running in a coroutine

```python
results: List = await scheduler.run()
```

#### Running (blocking)

```python
results: List = scheduler.run_blocking()
```

### Terminating a scheduler

To cancel a scheduler, use `terminate()`:

```python
scheduler.terminate()
```

A terminated scheduler will always return an empty list.

## Design

When the scheduler starts, it will simultaneously run a number of processes up to the value returned by `Scheduler.optimal_process_count()`. When a process finishes, another is started to take its place.

If `dynamic` is enabled, the scheduler will check the CPU usage periodically and increase the number of concurrent processes if the CPU usage is below the threshold.

This diagram demonstrates the implementation of `Scheduler`:

![Image demonstrating the implementation of Scheduler.](/docs/images/scheduler.png)

## Developer notes

### Packaging the project

From the [documentation](https://packaging.python.org/guides/distributing-packages-using-setuptools/#packaging-your-project):

```
rm -r dist/
python setup.py sdist
python setup.py bdist_wheel
twine upload dist/*
```
