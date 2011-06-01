#!/usr/bin/env python
import logging
import os
import Queue
import sys
import time
import threading
from logging.handlers import RotatingFileHandler
from optparse import make_option

from django.core.management.base import BaseCommand, CommandError
from django.db.models.loading import get_apps

from djutils.queue import autodiscover
from djutils.queue.exceptions import QueueException
from djutils.queue.queue import invoker, queue_name, registry
from djutils.utils.helpers import ObjectDict


class Command(BaseCommand):
    """
    Queue consumer.  Example usage::
    
    To start the consumer (note you must export the settings module):
    
    django-admin.py queue_consumer
    """
    
    help = "Run the queue consumer"
    option_list = BaseCommand.option_list + (
        make_option('--delay', '-d',
            dest='delay',
            default=0.1,
            type='float',
            help='Default interval between invoking, in seconds'
        ),
        make_option('--backoff', '-b',
            dest='backoff',
            default=1.15,
            type='float',
            help='Backoff factor when no message found'
        ),
        make_option('--max', '-m',
            dest='max_delay',
            default=60,
            type='int',
            help='Maximum time to wait, in seconds, between polling'
        ),
        make_option('--logfile', '-l',
            dest='logfile',
            default='',
            help='Destination for log file, e.g. /var/log/myapp.log'
        ),
        make_option('--no-periodic', '-n',
            dest='no_periodic',
            action='store_true',
            default=False,
            help='Do not enqueue periodic commands'
        ),
        make_option('--threads', '-t',
            dest='threads',
            default=1,
            type='int',
            help='Number of worker threads'
        ),
    )
    
    def initialize_options(self, options):
        self.queue_name = queue_name
        
        self.logfile = options.logfile or '/var/log/djutils-%s.log' % self.queue_name
        
        self.default_delay = options.delay
        self.max_delay = options.max_delay
        self.backoff_factor = options.backoff
        self.threads = options.threads
        self.periodic_commands = not options.no_periodic

        if self.backoff_factor < 1.0:
            raise CommandError('backoff must be greater than or equal to 1')
        
        if self.threads < 1:
            raise CommandError('threads must be at least 1')
         
        # initialize delay
        self.delay = self.default_delay
        
        self.logger = self.get_logger()
        
        # queue to track messages to be processed
        self._queue = Queue.Queue()
        
        # queue to track ids of threads that errored out
        self._errors = Queue.Queue()
        
        # list of worker threads
        self._threads = []
    
    def get_logger(self, verbosity=1):
        log = logging.getLogger('djutils.queue.logger')
        
        if verbosity == 2:
            log.setLevel(logging.DEBUG)
        elif verbosity == 1:
            log.setLevel(logging.INFO)
        else:
            log.setLevel(logging.WARNING)
        
        if not log.handlers:
            handler = RotatingFileHandler(self.logfile, maxBytes=1024*1024, backupCount=3)
            handler.setFormatter(logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s"))
            
            log.addHandler(handler)
        
        return log
    
    def start_periodic_command_thread(self):
        periodic_command_thread = threading.Thread(
            target=self.enqueue_periodic_commands
        )
        periodic_command_thread.daemon = True
        
        self.logger.info('Starting periodic command execution thread')
        periodic_command_thread.start()
        
        return periodic_command_thread
    
    def _queue_worker(self):
        """
        A worker thread that will chew on dequeued messages
        """
        while 1:
            message = self._queue.get()
            self._queue.task_done()
            
            try:
                command = registry.get_command_for_message(message)
                command.execute()
            except QueueException:
                # log error
                self.logger.warn('queue exception raised', exc_info=1)
            except:
                # put the thread's id into the queue of errors for removal
                current = threading.current_thread()
                self._errors.put(current.ident)
                
                # log the error and raise, killing the worker thread
                self.logger.error('exception encountered, exiting thread %s' % current, exc_info=1)
                raise
    
    def create_worker_thread(self):
        thread = threading.Thread(target=self._queue_worker)
        thread.daemon = True
        thread.start()
        self.logger.info('created thread "%s"' % (thread.ident))
        return thread
    
    def remove_dead_worker(self, ident):
        self.logger.info('removing thread "%s"' % (ident))
        self._threads = [w for w in self._threads if w.ident != ident]
    
    def check_worker_health(self):
        while not self._errors.empty():
            error_ident = self._errors.get()
            self.remove_dead_worker(error_ident)
            
            self._errors.task_done()
        
        while len(self._threads) < self.threads:
            self._threads.append(self.create_worker_thread())
    
    def initialize_threads(self):
        self.check_worker_health()

    def run_with_periodic_commands(self):
        """
        Pull messages from the queue so long as:
        - no unhandled exceptions when dequeue-ing and processing messages
        - no unhandled exceptions while enqueue-ing periodic commands
        """
        while 1:
            t = self.start_periodic_command_thread()
            
            while t.is_alive():
                self.check_worker_health()
                self.process_message()
            
            self.logger.error('Periodic command thread died')
    
    def run_only_queue(self):
        """
        Pull messages from the queue until shut down or an unhandled exception
        is encountered while dequeue-ing and processing messages
        """
        while 1:
            self.check_worker_health()
            self.process_message()
    
    def process_message(self):
        message = invoker.read()
        
        if message:
            self.logger.info('Processing: %s' % message)
            self.delay = self.default_delay
            self._queue.put(message)
            self._queue.join()
        else:
            if self.delay > self.max_delay:
                self.delay = self.max_delay
            
            self.logger.debug('No messages, sleeping for: %s' % self.delay)
            
            time.sleep(self.delay)
            self.delay *= self.backoff_factor
    
    def enqueue_periodic_commands(self):
        while True:
            start = time.time()
            self.logger.debug('Enqueueing periodic commands')
            
            try:
                invoker.enqueue_periodic_commands()
            except:
                self.logger.error('Error enqueueing periodic commands', exc_info=1)
                raise
            
            end = time.time()
            time.sleep(60 - (end - start))
    
    def handle(self, *args, **options):
        """
        Entry-point of the consumer -- in what might be a premature optimization,
        I've chosen to keep the code paths separate depending on whether the
        periodic command thread is started.
        """
        autodiscover()
        
        self.initialize_options(ObjectDict(options))
        
        self.logger.info('Initializing consumer with options:\nlogfile: %s\ndelay: %s\nbackoff: %s\nthreads: %s' % (
            self.logfile, self.delay, self.backoff_factor, self.threads))

        self.logger.info('Loaded classes:\n%s' % '\n'.join([
            klass for klass in registry._registry
        ]))
        
        try:
            if self.periodic_commands:
                self.run_with_periodic_commands()
            else:
                self.run_only_queue()
        except:
            self.logger.error('error', exc_info=1)
