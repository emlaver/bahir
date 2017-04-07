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

import time

import requests
import sys
from cloudant import Cloudant
from cloudant import CouchDB
from cloudant.design_document import DesignDocument

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

    def test_dbs_set_up(self):
        if os.environ.get('RUN_CLOUDANT_TESTS') is None:
            admin_party = False
            if (os.environ.get('ADMIN_PARTY') and
                os.environ.get('ADMIN_PARTY') == 'true'):
                admin_party = True
            self.user = os.environ.get('DB_USER', None)
            self.pwd = os.environ.get('DB_PASSWORD', None)
            self.url = os.environ['DB_URL']
            self.client = CouchDB(
                self.user,
                self.pwd,
                admin_party,
                url=self.url,
                connect=True
            )
        else:
            self.account = os.environ.get('CLOUDANT_ACCOUNT')
            self.user = os.environ.get('DB_USER')
            self.pwd = os.environ.get('DB_PASSWORD')
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

        #1) Delete test databases if exist
        #2) Create databases
        #3) Create required indices
        for db_name in TEST_DBS:
            #self.client.connect()
            #print('try to create ' + db_name)
            database = self.client[db_name]
            if not database.exists():
                self.db = self.client.create_database(db_name)
            self._create_index(db_name)
        #self._populate_db_with_customers()

    def test_dbs_tear_down(self):
        """
        for db_name in TEST_DBS:
            print('try to delete ' + db_name)
            database = self.client[db_name]
            if database.exists():
                self.client.delete_database(db_name)
        """

    def _populate_db_with_customers(self, doc_count=200, **kwargs):
        off_set = kwargs.get('off_set', 0)
        doc = None
        with open('../resources/schema_data/n_customer.json') as json_data:
            doc = json.loads(json_data)
        docs = [

            {'_id': 'julia{0:03d}'.format(i), 'name': 'julia', 'age': i}
            for i in range(off_set, off_set + doc_count)
        ]
        return self.db.bulk_docs(docs)

    def _create_index(self, db_name):
        """
        Create search index based on the defintion defined in db-index-func/<db_name>.txt
        """
        index_func_path = self._get_index_func_filepath(db_name)

        if os.path.isfile(index_func_path):
            # create index request payload from predefined file
            with open(index_func_path, 'r') as content_file:
                payload = content_file.read()
            self.db.create_document(payload)

    def _get_index_func_filepath(self, db_name):
        return os.path.join(os.path.dirname(__file__), "db-index-func", db_name + ".txt")

    def book_flights(self, user, toFlightId, retFlightId):
        """
        Login as the given user and booking flights.
        Set retFlightId=None if booking one way.
        """

    def get_flightId_by_number(self, flightNum):
        """
        Get the generated flight ID for the given flight number

        db = self.client['n_flights']
        url = "https://{}/{}".format(
            self.test_properties["cloudanthost"],
            "n_flight/_design/view/_search/n_flights?q=flightSegmentId:" + flightNum)
        param = {"q": "flightSegmentId:" + flightNum}

        #response = cloudantUtils.r.get(url, params=param)
        data = None #response.json()
        if int(data["total_rows"]) > 0:
            # just get one from the dict
            return data["rows"][0]["id"]
        else:
            raise RuntimeError("n_flights has no data for ", flightNum)
        """

    def load_SpecCharValuePredicateData(self):
        """
        Create booking data needed to test SpecCharValuePredicate
        """

        # book flights AA93 and AA330
        flight1 = "AA93"
        flight2 = "AA330"

        # Step#1 - need to find the flights generated _id required for booking
        flight1_id = self.get_flightId_by_number(flight1)
        print("{} id = {}".format(flight1, flight1_id))

        flight2_id = self.get_flightId_by_number(flight2)
        print("{} id = {}".format(flight2, flight2_id))

        # Step#2 - add the boooking
        self.book_flights("uid0@email.com", flight1, flight2)

    def sparksubmit(self):
        print('spark submit call')
        try:
            sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python"))
            sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python", "lib", "py4j-0.8.2.1-src.zip"))

        except KeyError:
            raise RuntimeError("Environment variable SPARK_HOME not set")

        return os.path.join(os.environ.get("SPARK_HOME"), "bin", "spark-submit")