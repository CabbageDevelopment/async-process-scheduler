#  MIT License
#
#  Copyright (c) 2020 Sam McCormack
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
from typing import Union, Any


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


def attach(manager, obj) -> Union["SharedMemoryObject", Any]:
    if is_ndarray(obj) and obj.size > 2:
        from numpy import ndarray

        arr = obj

        shared = manager.SharedMemory(size=arr.nbytes)
        sm_arr = ndarray(arr.shape, dtype=arr.dtype, buffer=shared.buf)

        sm_arr[:] = arr[:]  # Copy array into shared memory.

        del arr  # Encourage garbage collection of original array.

        name = shared.name
        # shared.close()

        return SharedMemoryObject(name, sm_arr.shape, sm_arr.dtype)

    return obj


def is_ndarray(obj):
    try:
        from numpy import ndarray
    except:
        # Not installed, use dummy type.
        ndarray = DummyNdarray

    return isinstance(obj, ndarray)
