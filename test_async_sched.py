import asyncio
import functools
import logging
import unittest
from concurrent import futures

import async_sched


def unittest_run_loop(func, *args, **kwargs):
    @functools.wraps(func, *args, **kwargs)
    def new_func(self, *inner_args, **inner_kwargs):
        return self.loop.run_until_complete(
            func(self, *inner_args, **inner_kwargs)
        )
    return new_func


class TestAsyncSched(unittest.TestCase):

    async def aio_func(self, delay):
        if delay == 0:
            await asyncio.sleep(0.1)
            raise Exception

        await asyncio.sleep(delay)

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.loop = asyncio.get_event_loop()
        self.sched = async_sched.AsyncSched(debug_mode=True)

    @unittest_run_loop
    async def test_1(self):
        """Test AsyncSched._sched_task.
        """
        task_id = 'test_task_id'
        task = self.sched._sched_task(task_id, self.aio_func(0.1))

        self.assertEqual(task, self.sched._tasks[task_id])
        self.assertEqual(len(self.sched._tasks), 2)

        await asyncio.wait_for(task, timeout=None)

        self.assertEqual(len(self.sched._tasks), 1)
        self.assertEqual(len(self.sched._errors), 0)

    @unittest_run_loop
    async def test_2(self):
        """Test canceling a task running.
        """
        task_id = 'test_task_id'
        task = self.sched.call_soon(task_id, self.aio_func, 0.1)

        self.assertEqual(task, self.sched._tasks[task_id])
        self.assertEqual(len(self.sched._tasks), 2)

        rtn = self.sched.cancel(task_id)

        self.assertEqual(rtn, True)

        with self.assertRaises(futures.CancelledError):
            await asyncio.wait_for(task, timeout=None)

        self.assertEqual(len(self.sched._tasks), 1)
        self.assertEqual(len(self.sched._errors), 0)

    @unittest_run_loop
    async def test_3(self):
        """Test cancelling a task pending
        """
        task_id = 'test_task_id'
        task = self.sched.call_later(task_id, 100, self.aio_func, 0.1)

        self.assertEqual(task, self.sched._tasks[task_id])
        self.assertEqual(len(self.sched._tasks), 2)

        self.sched.cancel(task_id)

        with self.assertRaises(futures.CancelledError):
            await asyncio.wait_for(task, timeout=None)

        self.assertEqual(len(self.sched._tasks), 1)
        self.assertEqual(len(self.sched._errors), 0)

    @unittest_run_loop
    async def test_4(self):
        """Test handling unexpected excetpion while running the task.
        """
        task_id = 'test_task_id'
        task = self.sched._sched_task(task_id, self.aio_func(0))

        self.assertEqual(task, self.sched._tasks[task_id])
        self.assertEqual(len(self.sched._tasks), 2)

        with self.assertRaises(Exception):
            await asyncio.wait_for(task, timeout=None)

        self.assertEqual(len(self.sched._tasks), 1)
        self.assertEqual(len(self.sched._errors), 1)

    @unittest_run_loop
    async def test_5(self):
        """Test deplicated task_id.
        """
        task_id = 'test_task_id'

        sched_funcs = [
            functools.partial(self.sched.call_periodic, task_id, 1,
                              self.aio_func, 1),
            functools.partial(self.sched.call_do_periodic, task_id, 1,
                              self.aio_func, 1),
            functools.partial(self.sched.call_later, task_id, 1,
                              self.aio_func, 1),
            functools.partial(self.sched.call_soon, task_id, self.aio_func, 1)
        ]

        for _sched_func in sched_funcs:

            task1 = _sched_func()

            self.assertEqual(task1, self.sched._tasks[task_id])
            self.assertEqual(len(self.sched._tasks), 2)

            task2 = _sched_func()

            self.assertEqual(task2, None)
            self.assertEqual(task1, self.sched._tasks[task_id])
            self.assertEqual(len(self.sched._tasks), 2)

            task1.cancel()

            with self.assertRaises(futures.CancelledError):
                await asyncio.wait_for(task1, timeout=None)

            self.assertEqual(len(self.sched._tasks), 1)
            self.assertEqual(len(self.sched._errors), 0)

    @unittest_run_loop
    async def test_6(self):
        """Test AsyncSched.teardown.
        """
        task_id = 'test_task_id'
        task1 = self.sched.call_periodic(task_id + '1', 5, self.aio_func, 1)
        task2 = self.sched.call_periodic(task_id + '2', 5, self.aio_func, 1)
        task3 = self.sched.call_periodic(task_id + '3', 5, self.aio_func, 1)
        task4 = self.sched.call_periodic(task_id + '4', 5, self.aio_func, 1)
        task5 = self.sched.call_periodic(task_id + '5', 5, self.aio_func, 1)

        self.assertEqual(len(self.sched._tasks), 6)
        self.assertEqual(self.sched._tasks['test_task_id1'], task1)
        self.assertEqual(self.sched._tasks['test_task_id2'], task2)
        self.assertEqual(self.sched._tasks['test_task_id3'], task3)
        self.assertEqual(self.sched._tasks['test_task_id4'], task4)
        self.assertEqual(self.sched._tasks['test_task_id5'], task5)

        rtn = await self.sched.teardown()

        await asyncio.sleep(0.5)  # Give a chacne to run callback.

        self.assertEqual(rtn, True)

        self.assertEqual(len(self.sched._tasks), 0)

    @unittest_run_loop
    async def test_7(self):
        """Test AsyncSched.call_soon.
        """
        task_id = 'test_task_id'
        task = self.sched.call_soon(task_id, self.aio_func, 0.1)

        self.assertEqual(task, self.sched._tasks[task_id])
        self.assertEqual(len(self.sched._tasks), 2)

        await asyncio.wait_for(task, timeout=None)

        self.assertEqual(len(self.sched._tasks), 1)
        self.assertEqual(len(self.sched._errors), 0)

    @unittest_run_loop
    async def test_8(self):
        """Test AsyncSched.call_later
        """
        task_id = 'test_task_id'
        task = self.sched.call_later(task_id, 1, self.aio_func, 0.1)

        self.assertEqual(task, self.sched._tasks[task_id])
        self.assertEqual(len(self.sched._tasks), 2)

        await asyncio.wait_for(task, timeout=None)

        self.assertEqual(len(self.sched._tasks), 1)
        self.assertEqual(len(self.sched._errors), 0)

    @unittest_run_loop
    async def test_9(self):
        """Test AsyncSched.call_periodic
        """
        task_id = 'test_task_id'
        task = self.sched.call_periodic(task_id, 5, self.aio_func, 0.1)

        self.assertEqual(task, self.sched._tasks[task_id])
        self.assertEqual(len(self.sched._tasks), 2)

        self.sched.cancel(task_id)

        with self.assertRaises(futures.CancelledError):
            await asyncio.wait_for(task, timeout=None)

        self.assertEqual(len(self.sched._tasks), 1)
        self.assertEqual(len(self.sched._errors), 0)

    @unittest_run_loop
    async def test_10(self):
        """Test AsyncSched.cancel
        """
        task_id = 'test_task_id'
        task = self.sched.call_later(task_id, 1, self.aio_func, 0.1)

        self.assertEqual(task, self.sched._tasks[task_id])
        self.assertEqual(len(self.sched._tasks), 2)

        self.assertEqual(self.sched.cancel(task_id), True)

        with self.assertRaises(futures.CancelledError):
            await asyncio.wait_for(task, timeout=None)

        self.assertEqual(len(self.sched._tasks), 1)
        self.assertEqual(len(self.sched._errors), 0)

    @unittest_run_loop
    async def test_11(self):
        """Test AsyncSched.cancel with pattern argument
        """
        task_id = 'test_task_id'
        task_id1 = task_id + '1'
        task_id2 = task_id + '2'
        task_id3 = task_id + '3'
        task1 = self.sched.call_later(task_id1, 1, self.aio_func, 0.1)
        task2 = self.sched.call_later(task_id2, 1, self.aio_func, 0.1)
        task3 = self.sched.call_later(task_id3, 1, self.aio_func, 0.1)

        self.assertEqual(task1, self.sched._tasks[task_id1])
        self.assertEqual(task2, self.sched._tasks[task_id2])
        self.assertEqual(task3, self.sched._tasks[task_id3])
        self.assertEqual(len(self.sched._tasks), 4)

        self.assertEqual(self.sched.cancel(task_id, True), [True, True, True])
        done, pending = await asyncio.wait((task1, task2, task3), timeout=None)

        self.assertEqual(len(pending), 0)
        self.assertEqual(len(done), 3)

        self.assertEqual(len(self.sched._tasks), 1)
        self.assertEqual(len(self.sched._errors), 0)
