#!/usr/bin/env python
# Copyright (c) 2017 IBM Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
test_base - The base class for all tests
"""
import json
import os
import unittest

import sys
from cloudant import Cloudant
from cloudant import CouchDB

from tests._test_util import TEST_DBS


class TestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.environ.get('RUN_CLOUDANT_TESTS') is None:
            if os.environ.get('DB_URL') is None:
                os.environ['DB_URL'] = 'http://127.0.0.1:5984'

        # Check that connector jar is found and available
        try:
            jarpath = os.environ["CONNECTOR_JAR"]
            if os.path.isfile(jarpath):
                print("Cloudant-Spark Connector path: ", jarpath)
            else:
                raise RuntimeError("Invalid Cloudant-Spark Connector path:", jarpath)

        except KeyError:
            raise RuntimeError("Environment variable CONNECTOR_JAR not set")

    def set_up_client(self):
        if os.environ.get('RUN_CLOUDANT_TESTS') is None:
            admin_party = False
            if (os.environ.get('ADMIN_PARTY') and
                os.environ.get('ADMIN_PARTY') == 'true'):
                admin_party = True
            self.user = os.environ.get('COUCHDB_USER', None)
            self.pwd = os.environ.get('COUCHDB_PASSWORD', None)
            self.url = os.environ['DB_URL']
            self.client = CouchDB(
                self.user,
                self.pwd,
                admin_party,
                url=self.url,
                connect=True
            )
        else:
            self.account = os.environ.get('CLOUDANT_USER')
            self.user = os.environ.get('CLOUDANT_USER')
            self.pwd = os.environ.get('CLOUDANT_PASSWORD')
            self.url = os.environ.get(
                'DB_URL',
                'https://{0}.cloudant.com'.format(self.account))
            self.client = Cloudant(
                self.user,
                self.pwd,
                url=self.url,
                x_cloudant_user=self.account,
                connect=True
            )

    def db_set_up(self):
        #2) Create databases
        #3) Create required indices
        for db_name in TEST_DBS:
            #self.client.connect()
            db = self.client._DATABASE_CLASS(self.client, db_name)
            if db.exists():
                db.delete()
            db.create()
            name = db_name + '.json'
            file = os.path.join('tests', 'resources', name)
            if os.path.isfile(file):
                with open(file) as data_file:
                    data = json.load(data_file)
                    db.bulk_docs(data)
        #self._create_index(db, db_name)
        #self._populate_db_with_customers()

    def db_tear_down(self):
        for db_name in TEST_DBS:
            db = self.client._DATABASE_CLASS(self.client, db_name)
            db.delete()
        self.client.disconnect()

    def _populate_db_with_customers(self, db, doc_count=200, **kwargs):
        off_set = kwargs.get('off_set', 0)
        doc = None
        with open('../resources/schema_data/n_customer.json') as json_data:
            doc = json.loads(json_data)
        docs = [

            {'_id': 'julia{0:03d}'.format(i), 'name': 'julia', 'age': i}
            for i in range(off_set, off_set + doc_count)
        ]
        return db.bulk_docs(docs)

    def _create_index(self, db, db_name):
        """
        Create search index based on the defintion defined in db-index-func/<db_name>.txt
        """
        index_func_path = self._get_index_func_filepath(db_name)

        if os.path.isfile(index_func_path):
            # create index request payload from predefined file
            with open(index_func_path, 'r') as content_file:
                payload = content_file.read()
            db.create_document(payload)

    def _get_index_func_filepath(self, db_name):
        return os.path.join(os.path.dirname(__file__), "db-index-func", db_name + ".txt")


    def sparksubmit(self):
        try:
            sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python"))
            sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python", "lib", "py4j-0.8.2.1-src.zip"))

        except KeyError:
            raise RuntimeError("Environment variable SPARK_HOME not set")

        return os.path.join(os.environ.get("SPARK_HOME"), "bin", "spark-submit")