#*******************************************************************************
# Copyright (c) 2015 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#******************************************************************************/
import requests
import os
import time

class CloudantDbUtils:
	"""
	Test database related functions
	"""

	test_dbs = [
		"n_airportcodemapping",
		"n_airportcodemapping2",
		"n_booking",
		"n_customer",
		"n_customersession",
		"n_flight",
		"n_flightsegment",
		"airportcodemapping_df"
	]
	
	def __init__(self, test_properties):
		self.r = requests.Session()
		self.r.headers = {'Content-Type': 'application/json'}
		self.r.auth = (test_properties["cloudantusername"], test_properties["cloudantpassword"])
		self.cloudanthost = test_properties["cloudanthost"]
		
	
	def create_index(self, db_name):
		"""
		Create search index based on the defintion defined in db-index-func/<db_name>.txt
		"""
		index_func_path = self._get_index_func_filepath(db_name)
		
		if os.path.isfile(index_func_path):
			# create index request payload from predefined file	
			with open(index_func_path, 'r') as content_file:
				payload = content_file.read()
		
			print ("Create index using function in: {}".format(index_func_path))
			url = "https://{}/{}/_design/view".format(
				self.cloudanthost, db_name)
			response = self.r.put(url, data=payload)
			assert response.status_code == 201
					
	
	def get_doc_count(self, db_name):
		url = "https://{}/{}".format(
			self.cloudanthost, db_name)
		response = self.r.get(url)
		metadata = response.json()
		return metadata["doc_count"]
		
		
	def wait_for_doc_count(self, db_name, expected, timeoutInMin):
		"""
		Wait for the given database to reach the target doc count or until the timeout setting is reached
		"""
		timeout = time.time() + timeoutInMin * 60
		
		while (time.time() < timeout):
			doc_count = self.get_doc_count(db_name)
			if doc_count < int(expected):
				time.sleep(5)
			else:
				print ("{} doc count = {}".format(db_name, doc_count))
				return
				
		raise RuntimeError("Timeout waiting for {} to reach count {} after {} min(s)".format(
			db_name, expected, timeoutInMin))
		
		
	def _get_index_func_filepath(self, db_name):
		return os.path.join(os.path.dirname(__file__), "db-index-func", db_name + ".txt")
		
	



		

	
