from luigi.worker import Worker, TaskProcess
from luigi.scheduler import DISABLED, DONE, FAILED, PENDING
from luigi.contrib.spark import PySparkTask
from luigi.event import Event
from luigi import notifications

from pyspark import SparkContext, SparkConf

import time, threading, os, logging, json, types

logger = logging.getLogger('luigi-interface')

class LuigiSparkTerminationException(Exception):
    pass

def _async_raise(tid, exctype):
    '''Raises an exception in the threads with id tid'''
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid,
                                                  ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # "if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")

class ThreadWithExc(threading.Thread):
    '''A thread class that supports raising exception in the thread from
       another thread.
    '''
    def _get_my_tid(self):
        """determines this (self's) thread id

        CAREFUL : this function is executed in the context of the caller
        thread, to get the identity of the thread represented by this
        instance.
        """
        if not self.isAlive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid

        # TODO: in python 2.6, there's a simpler way to do : self.ident

        raise AssertionError("could not determine the thread's id")

    def raiseExc(self, exctype):
        """Raises the given exception type in the context of this thread.

        If the thread is busy in a system call (time.sleep(),
        socket.accept(), ...), the exception is simply ignored.

        If you are sure that your exception should terminate the thread,
        one way to ensure that it works is:

            t = ThreadWithExc( ... )
            ...
            t.raiseExc( SomeException )
            while t.isAlive():
                time.sleep( 0.1 )
                t.raiseExc( SomeException )

        If the exception is to be caught by the thread, you need a way to
        check that your thread has caught it.

        CAREFUL : this function is executed in the context of the
        caller thread, to raise an excpetion in the context of the
        thread represented by this instance.
        """
        _async_raise( self._get_my_tid(), exctype )

class SparkTaskProcess(ThreadWithExc):

    """ Wraps execution of tasks that should share a common Spark context """

    def __init__(self, task, worker_id, result_queue, sparkContext, random_seed=False, worker_timeout=0,
                 tracking_url_callback=None):
        self.sparkContext = sparkContext
        self.task = task
        self.worker_id = worker_id
        self.result_queue = result_queue
        self.random_seed = random_seed
        self.tracking_url_callback = tracking_url_callback
        if task.worker_timeout is not None:
            worker_timeout = task.worker_timeout
        self.timeout_time = time.time() + worker_timeout if worker_timeout else None
        self.exitcode = 1
        super(SparkTaskProcess, self).__init__()

    def _run_get_new_deps(self):
        run_again = False
        try:
            self.task.setup_remote(self.sparkContext)
            task_gen = self.task.main(self.sparkContext)
        except TypeError as ex:
            if 'unexpected keyword argument' not in getattr(ex, 'message', ex.args[0]):
                raise
            run_again = True
        if run_again:
            task_gen = self.task.main(self.sparkContext)
        if not isinstance(task_gen, types.GeneratorType):
            return None

        next_send = None
        while True:
            try:
                if next_send is None:
                    requires = six.next(task_gen)
                else:
                    requires = task_gen.send(next_send)
            except StopIteration:
                return None

            new_req = flatten(requires)
            new_deps = [(t.task_module, t.task_family, t.to_str_params())
                        for t in new_req]
            if all(t.complete() for t in new_req):
                next_send = getpaths(requires)
            else:
                return new_deps

    def run(self):
        logger.info('[pid %s] Worker %s running   %s', os.getpid(), self.worker_id, self.task.task_id)

        if self.random_seed:
            # Need to have different random seeds if running in separate processes
            random.seed((os.getpid(), time.time()))

        status = FAILED
        expl = ''
        missing = []
        new_deps = []
        try:
            # Verify that all the tasks are fulfilled!
            missing = [dep.task_id for dep in self.task.deps() if not dep.complete()]
            if missing:
                deps = 'dependency' if len(missing) == 1 else 'dependencies'
                raise RuntimeError('Unfulfilled %s at run time: %s' % (deps, ', '.join(missing)))
            self.task.trigger_event(Event.START, self.task)
            t0 = time.time()
            status = None

            if self.task.run == NotImplemented:
                # External task
                # TODO(erikbern): We should check for task completeness after non-external tasks too!
                # This will resolve #814 and make things a lot more consistent
                status = DONE if self.task.complete() else FAILED
            else:
                new_deps = self._run_get_new_deps()
                status = DONE if not new_deps else PENDING

            if new_deps:
                logger.info(
                    '[pid %s] Worker %s new requirements      %s',
                    os.getpid(), self.worker_id, self.task.task_id)
            elif status == DONE:
                self.task.trigger_event(
                    Event.PROCESSING_TIME, self.task, time.time() - t0)
                expl = json.dumps(self.task.on_success())
                self.exitcode = 0
                logger.info('[pid %s] Worker %s done      %s', os.getpid(),
                            self.worker_id, self.task.task_id)
                self.task.trigger_event(Event.SUCCESS, self.task)

        except KeyboardInterrupt:
            raise
        except BaseException as ex:
            status = FAILED
            logger.exception("[pid %s] Worker %s failed    %s", os.getpid(), self.worker_id, self.task)
            self.task.trigger_event(Event.FAILURE, self.task, ex)
            raw_error_message = self.task.on_failure(ex)
            expl = json.dumps(raw_error_message)
            self._send_error_notification(raw_error_message)
        finally:
            self.result_queue.put(
                (self.task.task_id, status, expl, missing, new_deps))

    def _send_error_notification(self, raw_error_message):
        subject = "Luigi: %s FAILED" % self.task
        notification_error_message = notifications.wrap_traceback(raw_error_message)
        formatted_error_message = notifications.format_task_error(subject, self.task,
                                                                  formatted_exception=notification_error_message)
        notifications.send_error_email(subject, formatted_error_message, self.task.owner_email)

    def terminate(self, timeout=10):
        start = time.time()
        while self.is_alive() and time.time() - start < timeout:
            time.sleep(0.1)
            task.raiseExc(LuigiSparkTerminationException)


class SparkContextWorker(Worker):

    def __init__(self, createSparkContext=True, *args, **kwargs):
        kwargs["assistant"] = True
        super(SparkContextWorker, self).__init__(*args, **kwargs)
        self._createSparkContext = createSparkContext

         
    def __enter__(self):
        if self._createSparkContext:
            import sparkconfig
            conf = SparkConf()
            sparkconfig.config(conf)
            self.sparkConfig = conf
            self.sparkContext = SparkContext(conf=self.sparkConfig)
            self.sparkContext.__enter__()
        return super(SparkContextWorker, self).__enter__()

    def __exit__(self, type, value, traceback):
        if self._createSparkContext:
            self.sparkContext.__exit__(type, value, traceback)
        return super(SparkContextWorker, self).__exit__(type, value, traceback)

    def _keep_alive(self, *args):
        return True

    def _create_task_process(self, task):
        def update_tracking_url(tracking_url):
            self._scheduler.add_task(
                task_id=task.task_id,
                worker=self._id,
                status=RUNNING,
                tracking_url=tracking_url,
            )

        if isinstance(task, PySparkTask):
            return SparkTaskProcess(task, self._id, self._task_result_queue, self.sparkContext,
                    random_seed=bool(self.worker_processes > 1),
                    worker_timeout=self._config.timeout,
                    tracking_url_callback=update_tracking_url,
            )
        else:
            return super(SparkContextWorker, self)._create_task_process(task)

    def _generate_worker_info(self):
        args = super(SparkContextWorker, self)._generate_worker_info()
        args += [("sparkworker", True)]
        return args
