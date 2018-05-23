import asyncio
import functools
import logging
import traceback
from concurrent import futures
from typing import Awaitable, Callable, Dict, List, Optional

TaskId = str
SchedRtn = Optional[asyncio.Future]
AsyncFunc = Callable[..., Awaitable]


class AsyncSched:
    """
    Attributes:
        tasks: Task list current being scheduled.
        errors: Task list corrupted.
    """

    def __init__(self, loop=None, debug_mode: bool = False) -> None:
        self.logger = logging.getLogger('async_sched')

        if loop is None:
            loop = asyncio.get_event_loop()

        self._loop = loop
        self._errors: List[TaskId] = []
        self._tasks: Dict[TaskId, asyncio.Future] = {}

        if debug_mode:
            self._sched_task('_task_tracker', self._task_tracker())

    def cancel(self, task_id: TaskId, pattern: bool = False):
        """Cacncel sched with with specific ID

        Args:
            task_id: ID of task to be cancelled.
            pattern: If the arg is True, it cancels all tasks whose ID is
                in the parttern. It could be used to cancel all tasks relative
                to a specific `jackpot_id`.

                e.g. `1.cont_list` and `1.cont_amt` will be caancelled
                    by `1.` pattern.

        Todo:
            * Should it wait for being done?
        """
        if pattern is True:
            pattern_keys = [
                _key for _key in self._tasks.keys() if task_id in _key
            ]
            rtn_list = []

            for _key in pattern_keys:
                rtn_list.append(self.cancel(_key))
            return rtn_list

        task = self._tasks.get(task_id, None)
        if task is not None:
            rtn = task.cancel()
            self.logger.debug('Task(%s) is cancelled by cancel()' % task_id)
            return rtn

    async def teardown(self) -> bool:
        """
        Do:
            (1) Get `task_id`s.
            (2) Cancel all tasks.
            (3) Wait for the cancelation is done.

        Returns:
            - True, if it is done successfully.

        Note:
            - On Do(3), even if it requests to cancel a task, it couldn't be
                done immediately. It could take a little time.
            - Even `task.cancelled` returns `True`, it doesn't mean that
                the callback is also done.
        """
        task_ids = list(self._tasks.keys())[:]  # Do(1)

        for task_id in task_ids:
            task = self._tasks[task_id]
            task.cancel()  # Do(2)

            # Do(3)
            while not task.cancelled:
                await asyncio.sleep(0.5)  # pragma: no cover

            self.logger.info('Task(%s) is cancelled by del()' % task_id)

        return True

    def callback(self, task_id: TaskId, task: asyncio.Future) -> None:
        """Callback function for each tasks scheudled

        Do:
            (1) Get the result.
            (2) Handle task cnacelation.
            (3) If an exception happens, it put the task_id into `self.errros`.
            (4) Pop the task from `self.taskd`.
        """
        try:
            task.result()  # Do(1)

        # Do(2)
        except futures.CancelledError as e:
            self.logger.debug('Task(%s) is cancelled' % task_id)

        # Do(3)
        except Exception as e:
            self.logger.error(
                'Unexpected exception %s' % traceback.format_exc()
            )
            self._errors.append(task_id)

        else:
            self.logger.debug('Task(%s) is done' % task_id)

        finally:
            self._tasks.pop(task_id)  # Do(4)

    def _sched_task(
        self, task_id: TaskId, coro: Awaitable
    ) -> SchedRtn:
        """Run a task and add the task into `self._tasks` to manage.

        Do:
            (1) Create task future.
            (2) Check if is is duplicated one.
            (3) Add callback to the `self._tasks`.

        Retruns:
            - Scheduled task.
        """
        task = self._loop.create_task(coro)  # Do(1)

        # Do(2)
        if task_id in self._tasks:
            self.logger.error(
                'Duplicated task(%s) is tried to schedule' % task_id
            )
            return None

        call_back_ = functools.partial(self.callback, task_id)
        task.add_done_callback(call_back_)  # Do(3)
        self._tasks[task_id] = task
        return task

    async def _task_tracker(self):
        """With the purpose of debug, print the list of tasks scheduled.
        """

        self.logger.debug('Task tracker start')

        while True:
            await asyncio.sleep(1)
            self.logger.debug('Tasks  : %s' % list(self._tasks.keys()))
            self.logger.debug('Errors : %s' % self._errors)

    def call_later(
        self, task_id: TaskId, delay: float, aio_func: AsyncFunc, *args, **kwargs
    ) -> SchedRtn:
        """Schedule a task to be run after specific time.
        """
        async def later_func():
            await asyncio.sleep(delay)
            await aio_func(*args, **kwargs)

        task = self._sched_task(task_id, later_func())
        if task is not None:
            self.logger.debug(
                'Schedule task(%s), %d sec later' % (task_id, delay)
            )

        return task

    def call_periodic(
        self, task_id: TaskId, interval: float, aio_func: AsyncFunc, *args,
        **kwargs
    ) -> SchedRtn:
        """Schedule a task to be run at regular interval.
        The first sched happens after `args:interval`.
        """
        async def periodic_func():
            while True:
                await asyncio.sleep(interval)
                await aio_func(*args, **kwargs)

        task = self._sched_task(task_id, periodic_func())
        if task is not None:
            self.logger.debug(
                'Schedule task(%s), %d sec periodic' % (task_id, interval)
            )

        return task

    def call_do_periodic(
        self, task_id: TaskId, interval: float, aio_func: AsyncFunc, *args,
        **kwargs
    ) -> SchedRtn:
        """Schedule a task to be run at regular interval.
        The first sched happens at right next event loop order.
        """

        async def periodic_func():
            while True:
                await aio_func(*args, **kwargs)
                await asyncio.sleep(interval)

        task = self._sched_task(task_id, periodic_func())
        if task is not None:
            self.logger.debug(
                'Schedule task(%s), %d sec do-periodic' % (task_id, interval)
            )

        return task

    def call_soon(
        self, task_id: TaskId, aio_func: AsyncFunc, *args, **kwargs
    ) -> SchedRtn:
        """Schedule a task to be run at right next order of event loop
        """
        task = self._sched_task(task_id, aio_func(*args, **kwargs))
        if task is not None:
            self.logger.debug('Schedule task(%s)' % task_id)

        return task
