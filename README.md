# Holy grail of graceful shutdown in Python

Graceful shutdown should be an inseparable part of every serious application.
But graceful shutdowns are often hard, especially in the case of Python.
There are numerous questions on [StackOverflow](http://stackoverflow.com)
asking how to correctly catch `KeyboardInterrupt` and how to do an application
cleanup correctly.

The headaches will progress once you throw `asyncio` and `multiprocessing`
modules in the ring.

Let's try to figure it out!

### Intro

Almost all applications have three stages of execution:
* Initialization (usually in form of `start()`/`initialize()`/...)
* Actual execution
* Finalization (usually in form of `stop()`/`destroy()`/...)

What happens if application is instructed to be stopped during initialization?
What happens if application is instructed to be stopped during finalization?
Is it even safe to kill it during those stages?

In the provided solution, we shield the initialization and finalization
from termination - they always have to finish.  It should be a good practice
to make these two critical stages run as quick as possible (nobody likes
services that take minutes to start).  However, this is not always the case.
If - for example - initialization code tries to reach some remote file on
very slow network, it might take a while.  Such operations shouldn't be part
of initialization code, but rather part of the "actual execution".

This repository contains 2 scripts. Both scripts are similar at its core.
Both scripts have no external dependencies.
Both scripts have no side-effects (they don't create any files, they don't
make any network connections, ...), therefore you should not worry about
executing them.
They have been tested with **Python 3.8** on Windows & Linux.
They have very extensive logging.

The intention of these scripts is to **demonstrate** how it can be done,
and provide a reference from which you can take an inspiration (i.e.
copy-paste).
They are not intended to be used as packages.

Try to experiment with them!

**And most importantly, try to `Ctrl+C` them at any time during the execution!**

##### `simple.py`

A very simple `asyncio` application that implements graceful shutdown.

##### `complex.py`:

More complex application, that combines `asyncio`, `multiprocessing`
and `ThreadPoolExecutor`.  It is mere extension of the `simple.py`
script, but the core of the graceful shutdown remains the same.

The script demonstrates `DummyManager`, which - in real world scenario -
represents a class that does some pythonic "heavy lifting", i.e. does
some CPU intensive work.

It has 2 arbitrary "process" methods that simulate the heavy work.
It has also update() method, which manipulates with an internal state.

One real world example of this class might be
["Yara rules"](https://github.com/VirusTotal/yara-python) manager:
Instead of `process_string()` there would be something like `match()`
and `update()` would update the internal `yara.Rules` object.

With the help of `multiprocessing.Process`, this `DummyManager` is then
executed in several separate processes.  These process instances are then
managed by the `MultiProcessManager`.

The `MultiProcessManager` is then wrapped by an asynchronous service
`AsyncService1`, which executes its methods with the help of
`ThreadPoolExecutor`.

### Delay KeyboardInterrupt on initialization/finalization

When someone tries to SIGINT a Python process - directly or by pressing
`Ctrl+C` - the Python process injects a `KeyboardInterrupt` into a running
code.

If the `KeyboardInterrupt` is raised during initialization of your application,
it might have unwanted consequences, especially in complex application
(connections are not properly closed, not all content is written to a file,
...).  The same applies for finalization.  Apart from that, properly handling
(and potentially rollbacking) effects of initialization/finalization is hard.
Simply said - initialization & finalization is something you **don't** want to
interrupt.

Therefore in our application we implement exactly this: initialization and
finalization is shielded from interruption.  If SIGINT/SIGTERM was signaled
during initialization, execution of the signal handler is delayed until the
initialization is done.  If the application happens to be interrupted during
the initialization, then finalization is executed immediately after the
initialization is done.

```python
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
        print(f'!!! got KeyboardInterrupt during start')
        raise

    #
    # Application is started now and is running.
    # Wait for a termination event infinitelly.
    #

    self._wait()

except KeyboardInterrupt:
    #
    # The _stop() is also shielded from termination.
    #
    try:
        with DelayedKeyboardInterrupt():
            self._stop()
    except KeyboardInterrupt:
        print(f'!!! got KeyboardInterrupt during stop')
```

The `DelayedKeyboardInterrupt` is a context manager that suppresses
SIGINT & SIGTERM signal handlers for a block of code.  The signal handlers
are called on exit from the block.

It is inspired by [this StackOverflow comment](https://stackoverflow.com/a/21919644).

```python
SIGNAL_TRANSLATION_MAP = {
    signal.SIGINT: 'SIGINT',
    signal.SIGTERM: 'SIGTERM',
}


class DelayedKeyboardInterrupt:
    def __init__(self):
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
        print(f'!!! {SIGNAL_TRANSLATION_MAP[sig]} received; delaying KeyboardInterrupt')

```

### Asynchronous code

For graceful shutdown of asynchronous applications, you have to forget about
`asyncio.run()`. The behavior of `asyncio.run()` when `KeyboardInterrupt` is
raised is to cancel all tasks, wait for their cancellation (i.e. run their
`except asyncio.CancelledError` handlers) and then close the loop.

This is not always desired and most importantly, you don't have any control
over the order in which the tasks are cancelled.

The solution is to call an asynchronous finalizer function (e.g. you need
some kind of `async def astop()` function somewhere) when `KeyboardInterrupt`
is raised.  This way you have control over how each task gets cancelled.

### Beware of `ThreadPoolExecutor`

Keep in mind that when you schedule a function to be executed in the
`ThreadPoolExecutor`, the function will be executed until completion,
regardless of whether the `asyncio.get_running_loop().run_in_executor(...)`
task was cancelled.

It's probably obvious, but it is important to know this. If you schedule
too many functions into `ThreadPoolExecutor`, they won't get executed until
there's a thread ready to process them. If you fill all worker threads
in the `ThreadPoolExecutor` with functions that never return, no other
scheduled function will be executed.

This might be dangerous in situation where finalization is done in some
synchronous code (that must be scheduled by `run_in_executor()`), but the
executor is busy processing some other tasks - the finalization code
won't get chance to be executed.

```python
executor = ThreadPoolExecutor(max_workers=4)

def process():
    print('process')
    while True:
        time.sleep(1)

def stop():
    print('stop')

async def aprocess():
    print('aprocess')
    await asyncio.get_running_loop().run_in_executor(executor, process)

async def astop():
    print('astop')
    await asyncio.get_running_loop().run_in_executor(executor, stop)

async def amain():
    task_list = [ asyncio.create_task(aprocess()) for _ in range(4) ]
    
    #
    # asyncio.sleep(0) yields the execution and lets process
    # other tasks in the loop (like the ones we've just created).
    #
    await asyncio.sleep(0)
    
    #
    # Cancel the asyncio tasks.
    #
    for task in task_list:
        task.cancel()
    await asyncio.gather(*task_list, return_exceptions=True)
    
    #
    # Even though we've cancelled the asyncio tasks, the process()
    # functions are still being executed in the ThreadPoolExecutor.
    #
    # Because 4 tasks are now occupying the ThreadPoolExecutor infinitelly,
    # the next queued function in the executor won't get the chance to run.
    #

    await astop()

    #
    # We never get here!
    # (actually, we can get here - by cancelling the current task,
    # however it doesn't change the fact that the stop() function
    # will never be called.
    #
```

### Beware of Windows' buggy `ProactorEventLoop`

```python
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
        print(f'__loop_exception_handler: suppressing ConnectionResetError')
    elif type(context['exception']) == OSError:
        print(f'__loop_exception_handler: suppressing OSError')
    else:
        print(f'__loop_exception_handler: unhandled exception: {context}')

loop.set_exception_handler(__loop_exception_handler)
```

### Don't forget to catch `KeyboardInterrupt` in `multiprocessing.Process` workers

When application uses `multiprocessing.Process` and the application gets
interrupted, the signal handler is called in all children processes.
This effectively means that `KeyboardInterrupt` is injected into all processes.
If this exception is unhandled, the process is usually terminated but spits
a nasty exception log with traceback in the terminal (`stderr`).
  
If we want to get rid of this exception log, we should establish an exception
handler to catch the `KeyboardInterrupt` in the `multiprocessing.Process`
worker method (either `Process.run()` method, or the callback provided as the
`target` parameter) and then terminate the application.

```python
def _process_worker():
    try:
        __process_worker()
    except KeyboardInterrupt:
        print(f'[{multiprocessing.current_process().name}] ... Ctrl+C pressed, terminating ...')

def __process_worker():
    while True:
        time.sleep(1)

#
# ...
#

with DelayedKeyboardInterrupt():
    p = multiprocessing.Process(target=_process_worker)
    p.start()
```

### ... or ignore the `KeyboardInterrupt` in `multiprocessing.Process` workers completely

If you're certain that you're going to cleanly shutdown all the
`multiprocessing.Process` instances, you can choose to suppress the
`KeyboardInterrupt` in the process worker function.

```python
def _process_worker(stop_event: multiprocessing.Event):
    try:
        #
        # Because we have our own stop_event, we're going to suppress the
        # KeyboardInterrupt during the execution of the __process_worker().
        #
        # Note that if the parent process dies without setting the stop_event,
        # this process will be unresponsive to SIGINT/SIGTERM.
        # The only way to stop this process would be to ruthlessly kill it.
        #
        with DelayedKeyboardInterrupt():
            __process_worker(stop_event)

    #
    # Keep in mind that the KeyboardInterrupt will get delivered
    # after leaving from the DelayedKeyboardInterrupt() block.
    #
    except KeyboardInterrupt:
        print(f'[{multiprocessing.current_process().name}] ... Ctrl+C pressed, terminating ...')

def __process_worker(stop_event: multiprocessing.Event):
    stop_event.wait()

#
# ...
#

with DelayedKeyboardInterrupt():
    p = multiprocessing.Process(target=_process_worker)
    p.start()
```

### Synchronize start of the `multiprocessing.Process` workers

If the `KeyboardInterrupt` happens to be raised before the `target` worker
function is reached, we'd still get that nasty exception log.  If we want to
be sure we don't miss this exception, we need to synchronize the process
creation.

```python
def _process_worker(
        process_bootstrapped_event: multiprocessing.Event,
        stop_event: multiprocessing.Event
    ):
    try:
        #
        # Because we have our own stop_event, we're going to suppress the
        # KeyboardInterrupt during the execution of the __process_worker().
        #
        # Note that if the parent process dies without setting the stop_event,
        # this process will be unresponsive to SIGINT/SIGTERM.
        # The only way to stop this process would be to ruthlessly kill it.
        #
        with DelayedKeyboardInterrupt():
            process_bootstrapped_event.set()
            __process_worker(
                process_bootstrapped_event,
                stop_event
            )

    #
    # Keep in mind that the KeyboardInterrupt will get delivered
    # after leaving from the DelayedKeyboardInterrupt() block.
    #
    except KeyboardInterrupt:
        print(f'[{multiprocessing.current_process().name}] ... Ctrl+C pressed, terminating ...')

def __process_worker(
        process_bootstrapped_event: multiprocessing.Event,
        stop_event: multiprocessing.Event
    ):
    stop_event.wait()

#
# ...
#

with DelayedKeyboardInterrupt():
    process_bootstrapped_event = multiprocessing.Event()
    stop_event = multiprocessing.Event()
    p = multiprocessing.Process(target=_process_worker, args=(process_bootstrapped_event, stop_event))
    p.start()
    
    #
    # Set some meaningful timeout - we don't want to wait here
    # infinitelly if the process creation somehow failed.
    #
    process_bootstrapped_event.wait(5)

try:
    #
    # Let the process run for some time.
    #
    time.sleep(5)
except KeyboardInterrupt:
    print(f'... Ctrl+C pressed, terminating ...')
finally:
    #
    # And then stop it and wait for graceful termination.
    #
    with DelayedKeyboardInterrupt():
        stop_event.set()
        p.join()
```

### License

This software is open-source under the MIT license. See the LICENSE.txt file in this repository.

If you find this project interesting, you can buy me a coffee

```
  BTC 3GwZMNGvLCZMi7mjL8K6iyj6qGbhkVMNMF
  LTC MQn5YC7bZd4KSsaj8snSg4TetmdKDkeCYk
```
