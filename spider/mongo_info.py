from datetime import datetime, timedelta
from pymongo import MongoClient, errors

class MongoInfo:
	def __init__(self, database_name, client=None):
		self.database_name = database_name
		self.client = MongoClient('localhost', 27017) if client is None else client
		self.db = eval('self.client.%s'%database_name)

	def __getitem__(self, url):
		record = self.db.info.find_one({'_id': url})
		if record:
			return record
		else:
			raise KeyError(url + ' does not exist')

	def push(self, infodict):
		try:
			self.db.info.insert(infodict)
		except errors.DuplicateKeyError as e:
			pass 

	def clear(self):
		self.db.info.drop()