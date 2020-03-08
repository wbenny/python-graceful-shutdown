# =============================================================================
# DelayedKeyboardInterrupt implementation.
# This code can be moved into separate python package.
# =============================================================================

import os
import signal


__all__ = [
    'SIGNAL_TRANSLATION_MAP',
]

SIGNAL_TRANSLATION_MAP = {
    signal.SIGINT: 'SIGINT',
    signal.SIGTERM: 'SIGTERM',
}


class DelayedKeyboardInterrupt:
    def __init__(self, propagate_to_forked_processes=None):
        """
        Constructs a context manager that suppresses SIGINT & SIGTERM signal handlers
        for a block of code.

        The signal handlers are called on exit from the block.

        Inspired by: https://stackoverflow.com/a/21919644

        :param propagate_to_forked_processes: This parameter controls behavior of this context manager
        in forked processes.
        If True, this context manager behaves the same way in forked processes as in parent process.
        If False, signals received in forked processes are handled by the original signal handler.
        If None, signals received in forked processes are ignored (default).
        """
        self._pid = os.getpid()
        self._propagate_to_forked_processes = propagate_to_forked_processes
        self._sig = None
        self._frame = None
        self._old_signal_handler_map = None

    def __enter__(self):
        self._old_signal_handler_map = {
            sig: signal.signal(sig, self._handler)
            for sig, _ in SIGNAL_TRANSLATION_MAP.items()
        }

    def __exit__(self, exc_type, exc_val, exc_tb):
        for sig, handler in self._old_signal_handler_map.items():
            signal.signal(sig, handler)

        if self._sig is None:
            return

        self._old_signal_handler_map[self._sig](self._sig, self._frame)

    def _handler(self, sig, frame):
        self._sig = sig
        self._frame = frame

        #
        # Protection against fork.
        #
        if os.getpid() != self._pid:
            if self._propagate_to_forked_processes is False:
                print(f'!!! DelayedKeyboardInterrupt._handler: {SIGNAL_TRANSLATION_MAP[sig]} received; '
                      f'PID mismatch: {os.getpid()=}, {self._pid=}, calling original handler')
                self._old_signal_handler_map[self._sig](self._sig, self._frame)
            elif self._propagate_to_forked_processes is None:
                print(f'!!! DelayedKeyboardInterrupt._handler: {SIGNAL_TRANSLATION_MAP[sig]} received; '
                      f'PID mismatch: {os.getpid()=}, ignoring the signal')
                return
            # elif self._propagate_to_forked_processes is True:
            #   ... passthrough

        print(f'!!! DelayedKeyboardInterrupt._handler: {SIGNAL_TRANSLATION_MAP[sig]} received; delaying KeyboardInterrupt')


# =============================================================================
# Main script code.
# =============================================================================

import asyncio
import multiprocessing as mp
import queue
import random
import signal
import threading
import time
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from uuid import uuid4


#
# Maximum number of processes, threads and "busy tasks" for this script.
#
# Keep in mind that when there are more tasks scheduled to run
# in the threadpool executor than the THREADPOOL_EXECUTOR_MAX_WORKERS,
# the tasks are pending until all previous tasks are done.
#
# This becomes a problem becomes when the threadpool queue is full and
# e.g. run_in_executor(stop()) is scheduled.  In that case, the stop()
# won't happen before all other tasks are done.
#

PROCESS_WORKER_COUNT                = 4
THREADPOOL_EXECUTOR_MAX_WORKERS     = 8
BUSY_TASK_COUNT                     = THREADPOOL_EXECUTOR_MAX_WORKERS - 2  # leave space for scheduling stop()


class DummyManager:
    """
    This class represents a class that does some pythonic "heavy lifting",
    i.e. does some CPU intensive work.

    It has 2 arbitrary methods that simulate some heavy work.
    It has also update() method, which manipulates with an internal state.

    One real world example of this class might be "Yara rules" manager:
    Instead of process_string() there would be something like match()
    and update() would update the internal yara.Rules object.
    """

    def __init__(self):
        self._version = 1

    def process_string(self, parameter: str) -> dict:
        print(f'DummyManager.process_string({parameter=})')
        time.sleep(0.5)
        return {
            'version': self._version,
            'parameter': parameter
        }

    def process_number(self, parameter: int) -> dict:
        print(f'DummyManager.process_number({parameter=})')
        time.sleep(0.3)
        return {
            'version': self._version,
            'parameter': parameter
        }

    def update(self):
        print(f'DummyManager.update')
        time.sleep(5)
        self._version += 1


UNSET = object()


@dataclass
class MultiProcessManagerResultItem:
    event: threading.Event
    value: Any = UNSET


class MultiProcessManager:
    PROCESS_WORKER_BOOTSTRAP_TIMEOUT    = 5.0
    PROCESS_WORKER_START_TIMEOUT        = 30.0
    CALL_METHOD_TIMEOUT                 = 30.0
    UPDATE_TIMEOUT                      = 30.0

    def __init__(self, process_worker_count: int):
        #
        # Input & output queue.
        # Input queue contains tuples of (uuid, method_name, (args)).
        # Output queue contains tuples of (uuid, result).
        #
        self._input_queue = mp.Queue()                  # type: mp.Queue
        self._output_queue = mp.Queue()                 # type: mp.Queue

        #
        # Result map contains { uuid -> MultiProcessManagerResultItem } mapping.
        #
        self._result_map = {}                           # type: Dict[str, MultiProcessManagerResultItem]

        #
        # List of Process() workers and list of events that _process_worker sets
        # to signalize successful start.
        #
        self._process_list = []                         # type: List[mp.Process]
        self._process_bootstrapped_event_list = []      # type: List[mp.Event]
        self._process_started_event_list = []           # type: List[mp.Event]
        self._process_started_value_list = []           # type: List[mp.Value]

        #
        # When update() method is called, _update_condition notifies all waiters in the _process_worker.
        # After the update is done, each instance sets its own update_done_event.
        # _update_in_progress_lock protects the update() method from being called more than once.
        #
        self._update_condition_lock = mp.Lock()
        self._update_condition = mp.Condition(self._update_condition_lock)
        self._update_done_event_list = []               # type: List[mp.Event]
        self._update_in_progress_lock = threading.Lock()

        #
        # When _stop_event is set, all Process() instances are instructed to gracefully exit.
        #
        self._stop_event = mp.Event()

        #
        # Result collector thread.
        #
        self._result_collector_thread = threading.Thread(target=self._result_collector_thread_worker,
                                                         args=(self._output_queue, self._stop_event, self._result_map))

        for i in range(process_worker_count):
            process_bootstrapped_event = mp.Event()
            process_started_event = mp.Event()
            process_started_value = mp.Value('i', 0)
            update_done_event = mp.Event()

            process = mp.Process(target=self._process_worker,
                                 args=(self._input_queue, self._output_queue,
                                       process_bootstrapped_event, process_started_event, process_started_value,
                                       self._update_condition, update_done_event, self._stop_event))

            self._process_list.append(process)
            self._process_bootstrapped_event_list.append(process_bootstrapped_event)
            self._process_started_event_list.append(process_started_event)
            self._process_started_value_list.append(process_started_value)
            self._update_done_event_list.append(update_done_event)

    def start(self):
        print(f'MPM.start: starting')

        try:
            #
            # Start the process workers.
            #
            print(f'MPM.start: creating processes')
            for process in self._process_list:
                process.start()

            #
            # Wait until all processes reach the _process_worker() function.
            #
            print(f'MPM.start: waiting for "bootstrapped" events')
            self._wait_for_events(self._process_bootstrapped_event_list,
                                  self.PROCESS_WORKER_BOOTSTRAP_TIMEOUT)

            #
            # Wait until all processes are done initializing
            #
            print(f'MPM.start: waiting for "started" events')
            self._wait_for_events(self._process_started_event_list,
                                  self.PROCESS_WORKER_START_TIMEOUT)

            #
            # Check if all processes initialized successfully.
            #
            print(f'MPM.start: checking initialization status')
            for process_started_value in self._process_started_value_list:
                if process_started_value.value == 0:
                    print(f'MPM.start: process failed to start')
                    raise RuntimeError(f'Process initialization failed')

            #
            # Check if all processes are alive.
            #
            print(f'MPM.start: checking process status')
            for process in self._process_list:
                if not process.is_alive():
                    print('MPM.start: process killed')
                    raise RuntimeError(f'Process killed')

            #
            # If everything went fine, start the result collector thread.
            #
            print(f'MPM.start: starting result collector thread')
            self._result_collector_thread.start()
        except (TimeoutError, RuntimeError) as e:
            print(f'MPM.start: !!! start failed: {e}, killing all processes')
            for process in self._process_list:
                process.kill()
                process.close()

            self._process_list = []
            raise

        print(f'MPM.start: started')

    def stop(self):
        print(f'MPM.stop: stopping')

        #
        # First, set the stop event.
        #
        print(f'MPM.stop: waking up worker threads (CommandStop)')
        self._stop_event.set()

        #
        # Then wake up all worker threads in all processes.
        #
        print(f'MPM.stop: waking up worker threads (CommandUpdate)')
        with self._update_condition:
            self._update_condition.notify_all()

        print(f'MPM.stop: waking up worker threads (CommandCallMethod)')
        for process in self._process_list:
            self._input_queue.put((None, None, None))

        #
        # Wait until all queues are drained and then close them.
        #
        print(f'MPM.stop: closing input queue')
        self._input_queue.close()

        #
        # Wait until all processes terminate.
        #
        print(f'MPM.stop: waiting for processes to terminate')
        for process in self._process_list:
            process.join()
            process.close()

        #
        # Finally, wake up the result collector thread
        # and wait until it terminates.
        #
        print(f'MPM.stop: terminating result collector thread')
        self._output_queue.put((None, None))
        self._result_collector_thread.join()

        print(f'MPM.stop: closing output queue')
        self._output_queue.close()

        #
        # Wake up all waits in update().
        # Note that we ignore _update_in_progress_lock here.
        # The processes are already dead, so any wait on update_done_event
        # would timeout anyway.
        #
        print(f'MPM.stop: waking up update events')
        for update_done_event in self._update_done_event_list:
            update_done_event.set()

        #
        # Wake up all waits in _call_method().
        #
        print(f'MPM.stop: unprocessed tasks: {len(self._result_map)}')
        for uuid, result in self._result_map.items():
            result.event.set()

        print(f'MPM.stop: stopped')

    def update(self):
        with self._update_in_progress_lock:
            assert not self._stop_event.is_set()

            print('MPM.update: started')
            with self._update_condition:
                self._update_condition.notify_all()

            for update_done_event in self._update_done_event_list:
                update_done_event.wait(self.UPDATE_TIMEOUT)

            #
            # Check if the stop() method was called meanwhile
            # update was in progress.
            # If it was, it means the update_done_event was set
            # in the stop() method and the update actuall didn't
            # happen.
            #
            if self._stop_event.is_set():
                print('!!! MPM.update: stopped while updating')
                raise RuntimeError('Stopped while updating')

            for update_done_event in self._update_done_event_list:
                update_done_event.clear()

            print('MPM.update: finished')

    def process_string(self, parameter: str) -> dict:
        print(f'MPM.process_string({parameter=})')
        return self._call_method('process_string', (parameter, ))

    def process_number(self, parameter: int) -> dict:
        print(f'MPM.process_number({parameter=})')
        return self._call_method('process_number', (parameter, ))

    def _call_method(self, method_name: str, args: tuple):
        assert not self._stop_event.is_set()

        #
        # Enqueue RPC-like item in the input queue.
        # The process worker gets it from there.
        #
        print(f'MPM._call_method({method_name=}, {args=})')
        uuid = str(uuid4())
        event = threading.Event()
        result = MultiProcessManagerResultItem(event=event)
        self._result_map[uuid] = result

        #
        # Note that this call might raise ValueError() if the code is
        # is poorly synchronized and the input queue is already closed.
        #
        self._input_queue.put((uuid, method_name, args))
        print(f'MPM._call_method: task "{uuid}" enqueued, waiting')
        if not event.wait(self.CALL_METHOD_TIMEOUT):
            print(f'!!! MPM._call_method: task "{uuid}" timeouted')
            raise TimeoutError()

        if self._stop_event.is_set():
            print(f'MPM._call_method: service got stopped while waiting for the result')
            raise RuntimeError('Stopped while waiting for the result')

        print(f'MPM._call_method: task "{uuid}" done, {result.value=}')

        value = result.value
        del self._result_map[uuid]

        return value

    @staticmethod
    def _wait_for_events(event_list: List[mp.Event], timeout: float):
        deadline = time.time() + timeout
        for event in event_list:
            if not event.wait(deadline - time.time()):
                raise TimeoutError()

    @staticmethod
    def _result_collector_thread_worker(
            output_queue: mp.Queue,
            stop_event: mp.Event,
            result_map: Dict[str, MultiProcessManagerResultItem]
    ):
        print(f'MPM[collector]: started')

        while True:
            uuid, value = output_queue.get()

            if stop_event.is_set():
                #
                # There still might be valid queued items in the queue,
                # therefore we don't assert on the uuid/value.
                #
                # However, we ignore what's left in the queue and stop
                # unconditionally.
                #
                # assert uuid is None
                # assert value is None
                print(f'MPM[collector]: stopping')
                break

            print(f'MPM[collector]: collecting result {uuid}')
            result = result_map[uuid]
            result.value = value
            result.event.set()

        print(f'MPM[collector]: stopped')

    @staticmethod
    def _process_worker(
            input_queue: mp.Queue,
            output_queue: mp.Queue,
            process_bootstrapped_event: mp.Event,
            process_started_event: mp.Event,
            process_started_value: mp.Value,
            update_condition: mp.Condition,
            update_done_event: mp.Event,
            stop_event: mp.Event
    ):
        try:
            #
            # Worker function reached - signalize that bootstrapping phase
            # is done.
            #
            print(f'MPM[{mp.current_process().name}]: bootstrapped')
            process_bootstrapped_event.set()

            MultiProcessManager.__process_worker(
                input_queue,
                output_queue,
                process_bootstrapped_event,
                process_started_event,
                process_started_value,
                update_condition,
                update_done_event,
                stop_event
            )
        except KeyboardInterrupt:
            print(f'!!! MPM[{mp.current_process().name}]: KeyboardInterrupt')
            pass

    @staticmethod
    def __process_worker(
            input_queue: mp.Queue,
            output_queue: mp.Queue,
            process_bootstrapped_event: mp.Event,
            process_started_event: mp.Event,
            process_started_value: mp.Value,
            update_condition: mp.Condition,
            update_done_event: mp.Event,
            stop_event: mp.Event
    ):
        class StopProcessWorkerException(Exception):
            pass

        #
        # RPC-like commands.
        # Each command must have one process() method
        # and one worker() static method.
        #
        # Each worker is then executed in separated thread.
        #

        @dataclass
        class Command:
            def process(self):
                pass

            @staticmethod
            def worker():
                raise NotImplementedError()

        @dataclass
        class CommandCallMethod(Command):
            """
            This command represents a RPC-like message constructed in
            the _call_method().  It causes to call specified method
            in the DummyManager, and return the result in the output_queue.
            """
            uuid: str
            method_name: str
            args: tuple

            def process(self):
                print(f'MPM[{mp.current_process().name}]: CommandCallMethod.process({self.uuid=}, {self.method_name=}, {self.args=})')
                method = getattr(manager, self.method_name)
                value = method(*self.args)
                output_queue.put((self.uuid, value))

            @staticmethod
            def worker():
                while True:
                    uuid, method_name, args = input_queue.get()

                    if stop_event.is_set():
                        assert uuid is None
                        assert method_name is None
                        assert args is None
                        print(f'MPM[{mp.current_process().name}]: stopping CommandCallMethod.worker()')
                        break

                    command_queue.put(
                        PrioritizedItem(priority=3,
                                        command=CommandCallMethod(uuid, method_name, args))
                    )

        @dataclass
        class CommandUpdate(Command):
            """
            This command causes the DummyManager to update.
            When the update_condition is fired, all process
            workers perform an update at the same time.

            Note that this is different from CommandCallMethod,
            where there is no control over which Process will get
            the command.

            This command has higher priority than CommandCallMethod.
            """
            def process(self):
                print(f'MPM[{mp.current_process().name}]: CommandUpdate.process()')
                manager.update()
                update_done_event.set()

            @staticmethod
            def worker():
                with update_condition:
                    while True:
                        update_condition.wait()

                        if stop_event.is_set():
                            print(f'MPM[{mp.current_process().name}]: stopping CommandUpdate.worker()')
                            break

                        command_queue.put(
                            PrioritizedItem(priority=2,
                                            command=CommandUpdate())
                        )

        @dataclass
        class CommandStop(Command):
            """
            This command causes the process worker to stop.

            It has the highest priority.
            """
            def process(self):
                print(f'MPM[{mp.current_process().name}] CommandStop.process()')
                raise StopProcessWorkerException()

            @staticmethod
            def worker():
                stop_event.wait()

                print(f'MPM[{mp.current_process().name}]: stopping CommandStop.worker()')

                command_queue.put(
                    PrioritizedItem(priority=1,
                                    command=CommandStop())
                )

        command_list = [
            CommandCallMethod,
            CommandUpdate,
            CommandStop
        ]

        @dataclass(order=True)
        class PrioritizedItem:
            priority: int
            command: Command = field(compare=False)

        #
        # This queue is filled by command worker threads.
        #

        command_queue = queue.PriorityQueue()           # type: queue.PriorityQueue[Command]

        # =====================================================================
        # Main code.
        # =====================================================================

        manager = DummyManager()

        #
        # Create command worker threads.
        #

        print(f'MPM[{mp.current_process().name}]: creating command worker threads')
        thread_list = [
            threading.Thread(target=command.worker)
            for command in command_list
        ]

        try:
            print(f'MPM[{mp.current_process().name}]: starting command worker threads')
            for thread in thread_list:
                thread.start()

            #
            # Initialization is done.
            # Set process_started_value to non-zero value to signalize success
            # and set the process_started_event.
            #

            process_started_value.value = 1
            process_started_event.set()
            print(f'MPM[{mp.current_process().name}]: initialization done')

            while True:
                try:
                    item = command_queue.get()
                    try:
                        item.command.process()
                    except StopProcessWorkerException:
                        print(f'!!! MPM[{mp.current_process().name}]: stopping _process_worker()')
                        break
                except KeyboardInterrupt:
                    print(f'!!! MPM[{mp.current_process().name}]: KeyboardInterrupt (inner2)')
                    raise
                else:
                    command_queue.task_done()
        except KeyboardInterrupt:
            print(f'!!! MPM[{mp.current_process().name}]: KeyboardInterrupt (inner1)')
            raise
        finally:

            #
            # Gracefully wait until all threads terminate.
            #

            print(f'MPM[{mp.current_process().name}]: waiting for thread cleanup ...')
            for thread in thread_list:
                if thread.is_alive():
                    thread.join()
            print(f'MPM[{mp.current_process().name}]: ... terminated')


class AsyncService1:
    """
    Asynchronous service that wraps the MultiProcessManager.
    """

    def __init__(self, executor: Executor):
        self._executor = executor
        self._mpm = MultiProcessManager(process_worker_count=PROCESS_WORKER_COUNT)
        self._update_task = None                        # type: Optional[asyncio.Task]
        self._process_worker_task_list = []             # type: List[asyncio.Task]

    async def start(self):
        print(f'AsyncService1: starting')

        print(f'AsyncService1: starting MPM')
        await asyncio.get_running_loop().run_in_executor(self._executor,
                                                         self._mpm.start)

        print(f'AsyncService1: creating update task')
        self._update_task = asyncio.create_task(self._update_task_worker())

        print(f'AsyncService1: creating process worker tasks')
        for i in range(BUSY_TASK_COUNT):
            self._process_worker_task_list.append(
                asyncio.create_task(self._process_worker(i * 1000))
            )
        print(f'AsyncService1: started')

    async def stop(self):
        print(f'AsyncService1: stopping')

        print(f'AsyncService1: cancelling update task')
        self._update_task.cancel()
        await self._update_task

        print(f'AsyncService1: cancelling process worker tasks')
        for process_worker_task in self._process_worker_task_list:
            process_worker_task.cancel()
        await asyncio.gather(*self._process_worker_task_list, return_exceptions=True)

        print(f'AsyncService1: stopping MPM')
        await asyncio.get_running_loop().run_in_executor(self._executor,
                                                         self._mpm.stop)
        print(f'AsyncService1: stopped')

    async def update(self):
        print(f'AsyncService1: updating')
        await asyncio.get_running_loop().run_in_executor(self._executor, self._mpm.update)
        print(f'AsyncService1: updated')

    async def process_string(self, parameter: str) -> dict:
        return await asyncio.get_running_loop().run_in_executor(self._executor,
                                                                self._mpm.process_string,
                                                                parameter)

    async def process_number(self, parameter: int) -> dict:
        return await asyncio.get_running_loop().run_in_executor(self._executor,
                                                                self._mpm.process_number,
                                                                parameter)

    #
    # Two versions of _update_task_worker:
    #   - unshielded: when this task is cancelled, it is _really_ cancelled;
    #                 if the cancellation happens in the middle of the update,
    #                 the task doesn't wait until it finishes.
    #
    #                 But keep in mind that the task DOESN'T get cancelled in
    #                 the ThreadPoolExecutor - therefore cancellation of this
    #                 task doesn't make instantly an empty space there.
    #
    #  - shielded:    when this task is cancelled in the middle of the update,
    #                 it waits for that update to finish.
    #
    # Feel free to experiment with both of them and chose what suits you.
    #

    async def __update_task_worker_unshielded(self):
        try:
            while True:
                await asyncio.sleep(10)
                await self.update()
        except asyncio.CancelledError:
            print(f'!!! AsyncService1._update_task_worker: cancelled')

    async def __update_task_worker_shielded(self):
        update_task = None                              # type: Optional[asyncio.Task]

        try:
            while True:
                await asyncio.sleep(10)

                update_task = asyncio.create_task(self.update())
                await asyncio.shield(update_task)
        except asyncio.CancelledError:
            print(f'!!! AsyncService1._update_task_worker: cancelled')

            if update_task:
                print(f'!!! AsyncService1._update_task_worker: awaiting update_task')
                await update_task

    _update_task_worker = __update_task_worker_shielded

    #
    # Two versions of _process_worker.
    # Same rules as with _update_task_worker apply here.
    #

    async def __process_worker_unshielded(self, parameter: int):
        try:
            while True:
                #await asyncio.sleep(random.random() * 50)
                await asyncio.sleep(1)

                await self.process_number(parameter)
                await self.process_string(f'string-{parameter}')

                await asyncio.sleep(1)

                parameter += 1
        except asyncio.CancelledError:
            print(f'AsyncService1._process_worker: cancelled')

    async def __process_worker_shielded(self, parameter: int):
        task_process_number = None                      # type: Optional[asyncio.Task]
        task_process_string = None                      # type: Optional[asyncio.Task]

        try:
            while True:
                #await asyncio.sleep(random.random() * 50)
                await asyncio.sleep(1)

                task_process_number = asyncio.create_task(self.process_number(parameter))
                await task_process_number

                task_process_string = asyncio.create_task(self.process_string(f'string-{parameter}'))
                await task_process_string

                await asyncio.sleep(1)

                parameter += 1
        except asyncio.CancelledError:
            print(f'AsyncService1._process_worker: cancelled')

            if task_process_number:
                print(f'AsyncService1._process_worker: awaiting task_process_number')
                await task_process_number

            if task_process_string:
                print(f'AsyncService1._process_worker: awaiting task_process_string')
                await task_process_string

    _process_worker = __process_worker_shielded


class AsyncService2:
    """
    Dummy service that does nothing.
    """
    def __init__(self, executor: Executor):
        pass

    async def start(self):
        print(f'AsyncService2: starting')
        await asyncio.sleep(1)
        print(f'AsyncService2: started')

    async def stop(self):
        print(f'AsyncService2: stopping')
        await asyncio.sleep(1)
        print(f'AsyncService2: stopped')


class AsyncApplication:
    def __init__(self):
        self._loop = None                               # type: Optional[asyncio.AbstractEventLoop]
        self._wait_event = None                         # type: Optional[asyncio.Event]
        self._wait_task = None                          # type: Optional[asyncio.Task]

        self._executor = None                           # type: Optional[Executor]
        self._service1 = None                           # type: Optional[AsyncService1]
        self._service2 = None                           # type: Optional[AsyncService2]

    def run(self):
        self._loop = asyncio.new_event_loop()
        self._executor = ThreadPoolExecutor(max_workers=THREADPOOL_EXECUTOR_MAX_WORKERS)

        try:
            #
            # Shield _start() from termination.
            #

            try:
                with DelayedKeyboardInterrupt():
                    self._start()

            #
            # If there was an attempt to terminate the application,
            # the KeyboardInterrupt is raised AFTER the _start() finishes
            # its job.
            #
            # In that case, the KeyboardInterrupt is re-raised and caught in
            # exception handler below and _stop() is called to clean all resources.
            #
            # Note that it might be generally unsafe to call stop() methods
            # on objects that are not started properly.
            # This is the main reason why the whole execution of _start()
            # is shielded.
            #

            except KeyboardInterrupt:
                print(f'!!! AsyncApplication.run: got KeyboardInterrupt during start')
                raise

            #
            # Application is started now and is running.
            # Wait for a termination event infinitelly.
            #

            print(f'AsyncApplication.run: entering wait loop')
            self._wait()
            print(f'AsyncApplication.run: exiting wait loop')

        except KeyboardInterrupt:
            #
            # The _stop() is also shielded from termination.
            #
            try:
                with DelayedKeyboardInterrupt():
                    self._stop()
            except KeyboardInterrupt:
                print(f'!!! AsyncApplication.run: got KeyboardInterrupt during stop')
        finally:
            print(f'AsyncApplication.run: shutting down executor')
            self._executor.shutdown()

    async def _astart(self):
        self._service1 = AsyncService1(self._executor)
        self._service2 = AsyncService2(self._executor)

        await self._service1.start()
        await self._service2.start()

    async def _astop(self):
        await self._service2.stop()
        await self._service1.stop()

    async def _await(self):
        self._wait_event = asyncio.Event()
        self._wait_task = asyncio.create_task(self._wait_event.wait())
        await self._wait_task

    def _start(self):
        self._loop.run_until_complete(self._astart())

    def _stop(self):
        self._loop.run_until_complete(self._astop())

        #
        # Because we want clean exit, we patiently wait for completion
        # of the _wait_task (otherwise this task might get cancelled
        # in the _cancell_all_tasks() method - which wouldn't be a problem,
        # but it would be dirty).
        #
        # The _wait_event & _wait_task might not exist if the application
        # has been terminated before calling _wait(), therefore we have to
        # carefully check for their presence.
        #

        if self._wait_event:
            self._wait_event.set()

        if self._wait_task:
            self._loop.run_until_complete(self._wait_task)

        #
        # Before the loop is finalized, we setup an exception handler that
        # suppresses several nasty exceptions.
        #
        # ConnectionResetError
        # --------------------
        # This exception is sometimes raised on Windows, possibly because of a bug in Python.
        #
        # ref: https://bugs.python.org/issue39010
        #
        # When this exception is raised, the context looks like this:
        # context = {
        #     'message': 'Error on reading from the event loop self pipe',
        #     'exception': ConnectionResetError(
        #         22, 'The I/O operation has been aborted because of either a thread exit or an application request',
        #         None, 995, None
        #       ),
        #     'loop': <ProactorEventLoop running=True closed=False debug=False>
        # }
        #
        # OSError
        # -------
        # This exception is sometimes raised on Windows - usually when application is
        # interrupted early after start.
        #
        # When this exception is raised, the context looks like this:
        # context = {
        #     'message': 'Cancelling an overlapped future failed',
        #     'exception': OSError(9, 'The handle is invalid', None, 6, None),
        #     'future': <_OverlappedFuture pending overlapped=<pending, 0x1d8937601f0>
        #                 cb=[BaseProactorEventLoop._loop_self_reading()]>,
        # }
        #

        def __loop_exception_handler(loop, context: Dict[str, Any]):
            if type(context['exception']) == ConnectionResetError:
                print(f'!!! AsyncApplication._stop.__loop_exception_handler: suppressing ConnectionResetError')
            elif type(context['exception']) == OSError:
                print(f'!!! AsyncApplication._stop.__loop_exception_handler: suppressing OSError')
            else:
                print(f'!!! AsyncApplication._stop.__loop_exception_handler: unhandled exception: {context}')

        self._loop.set_exception_handler(__loop_exception_handler)

        try:
            #
            # Cancel all remaining uncompleted tasks.
            # We should strive to not make any, but mistakes happen and laziness
            # is also a thing.
            #
            # Generally speaking, cancelling tasks shouldn't do any harm (unless
            # they do...).
            #
            self._cancel_all_tasks()

            #
            # Shutdown all active asynchronous generators.
            #
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        finally:
            #
            # ... and close the loop.
            #
            print(f'AsyncApplication._stop: closing event loop')
            self._loop.close()

    def _wait(self):
        self._loop.run_until_complete(self._await())

    def _cancel_all_tasks(self):
        """
        Cancel all tasks in the loop.

        This method injects an asyncio.CancelledError exception
        into all tasks and lets them handle it.

        Note that after cancellation, the event loop is executed again and
        waits for all tasks to complete the cancellation.  This means that
        if some task contains code similar to this:

        >>> except asyncio.CancelledError:
        >>>     await asyncio.Event().wait()

        ... then the loop doesn't ever finish.
        """

        #
        # Code kindly borrowed from asyncio.run().
        #

        to_cancel = asyncio.tasks.all_tasks(self._loop)
        print(f'AsyncApplication._cancell_all_tasks: cancelling {len(to_cancel)} tasks ...')

        if not to_cancel:
            return

        for task in to_cancel:
            task.cancel()

        self._loop.run_until_complete(
            asyncio.tasks.gather(*to_cancel, loop=self._loop, return_exceptions=True)
        )

        for task in to_cancel:
            if task.cancelled():
                continue

            if task.exception() is not None:
                self._loop.call_exception_handler({
                    'message': 'unhandled exception during Application.run() shutdown',
                    'exception': task.exception(),
                    'task': task,
                })


def main():
    print(f'main: begin')
    app = AsyncApplication()
    app.run()
    print(f'main: end')


if __name__ == '__main__':
    main()
