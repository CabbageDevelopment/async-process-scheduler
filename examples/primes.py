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
from multiprocessing import Queue, Process
from typing import List, Tuple

import numpy as np
from numpy import ndarray

from scheduler.Scheduler import Scheduler
from scheduler.Task import Task


def calc_primes(numbers: ndarray) -> ndarray:
    output = np.empty(numbers.shape, dtype=np.bool)

    for i in range(len(numbers)):
        number = numbers[i]
        prime = True
        for j in range(2, number):
            if number % j == 0:
                prime = False
                break

        output[i] = prime

    return output


def mp_calc_primes(numbers: ndarray, queue: Queue) -> None:
    result = calc_primes(numbers)
    queue.put(result)


def on_progress(done, total):
    print(f"{done} of {total} tasks finished.")


async def coro_run():
    num_tasks = 32
    nums_per_task = int(100_000 // num_tasks)

    # Choose so that each row of the Numpy array is the same length.
    upper_limit = num_tasks * nums_per_task
    print(f"Finding all prime numbers below {upper_limit}.")

    data = np.empty((num_tasks, nums_per_task), dtype=np.int64)
    scheduler = Scheduler(progress_callback=on_progress)

    for i in range(num_tasks):
        data[i, :] = np.arange(i + 2, upper_limit + 2, num_tasks)

        queue = Queue()
        process = Process(target=mp_calc_primes, args=(data[i, :], queue))

        scheduler.add_task(Task(process, queue))

    result: List[Tuple[ndarray]] = await scheduler.run()

    primes = [None for _ in result]
    for i in range(num_tasks):
        primes[i] = data[i, result[i]]

    primes = np.concatenate(primes)
    primes = np.sort(primes)

    # Now that we have the results, we could show them in the GUI. For simplicity, let's print them.
    print(primes)


async def coro_sleep():
    # Note: time.sleep() would block the main thread and Scheduler would be unable to run.
    await asyncio.sleep(1e5)


if __name__ == "__main__":
    asyncio.ensure_future(coro_run())
    print("The main thread is not blocked.")

    # Simulate other code running, e.g. GUI. (Without this, the program will exit.)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(coro_sleep())
