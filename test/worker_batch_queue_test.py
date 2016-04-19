# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging
import os
from helpers import LuigiTestCase
from luigi import Task, Parameter, IntParameter
from luigi.worker import Worker


class DummyBatchTask(Task):

    task_num = IntParameter(default=1)
    batch_queue = Parameter(significant=False)

    def __init__(self, *args, **kwargs):
        super(DummyBatchTask, self).__init__(*args, **kwargs)
        self.has_run = False

    def complete(self):
        return self.has_run

    def run(self):
        logging.debug("%s - setting has_run", self)
        self.has_run = True


class BatchWorker(Worker):

    def __init__(self, *args, **kwargs):
        batch_queue = kwargs.pop('batch_queue')
        if batch_queue:
            # Set environment variable to mimic running on a cluster
            os.environ['LSB_QUEUE'] = batch_queue
        else:
            del os.environ['LSB_QUEUE']

        super(BatchWorker, self).__init__(*args, **kwargs)


class WorkerBatchQueueTest(LuigiTestCase):

    def test_ignore_other_queue(self):
        task = DummyBatchTask(batch_queue='some_queue')
        worker = BatchWorker(batch_queue='other_queue')
        worker.add(task)
        worker.run()

        self.assertFalse(task.complete())

    def test_process_own_queue(self):
        task = DummyBatchTask(batch_queue='same_queue')
        worker = BatchWorker(batch_queue='same_queue')
        worker.add(task)
        worker.run()

        self.assertTrue(task.complete())
