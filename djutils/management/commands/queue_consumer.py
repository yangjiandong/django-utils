#!/usr/bin/env python

from gevent import monkey, pool, queue; monkey.patch_all()
import gevent
import logging
import os
import sys
import time
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
            help='Number of worker threads (uses greenlets)'
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
        
        self.logger = self.get_logger(int(options.verbosity))
        
        # queue to track messages to be processed
        self._queue = queue.JoinableQueue()
        self._pool = pool.Pool(self.threads)
    
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
        self.logger.info('Starting periodic command execution thread')
        return gevent.spawn(self.enqueue_periodic_commands)

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
    
    def start_processor_thread(self):
        self.logger.info('Starting processor thread')
        return gevent.spawn(self.processor)
    
    def processor(self):
        while 1:
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
    
    def start_scheduler(self):
        self.logger.info('Starting scheduler thread')
        return gevent.spawn(self.scheduler)
    
    def scheduler(self):
        while 1:
            self._pool.wait_available()
            
            self.logger.debug('Fetching job from job queue.')
            job = self._queue.get()
            
            self._pool.spawn(self.worker, job)
            self._queue.task_done()
    
    def worker(self, message):
        try:
            command = registry.get_command_for_message(message)
            command.execute()
        except QueueException:
            # log error
            self.logger.warn('queue exception raised', exc_info=1)
        except:
            # log the error and raise, killing the worker
            self.logger.error('exception encountered, exiting thread %s' % gevent.getcurrent(), exc_info=1)
            raise
    
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
                self.start_periodic_command_thread()
            
            t = self.start_scheduler()
            self.start_processor_thread()
            t.join()
        except:
            self.logger.error('error', exc_info=1)
