import sys
import json
from pymongo import MongoClient
import pyorient

def parse(fh):
	def encode(text):
		return text.encode('utf-8');

	def getId(person):
		return str(person.get('id'))

	def getGender(person):
		return str(person.get('gender').get('type'))

	def getBirthName(person):
		def birthNameFilter(name):
			return "http://gedcomx.org/BirthName" == name.get('type')
		def fullNameFilter(nameForm):
			return 'fullText' in nameForm
		if person.get('names') is None or len(person.get('names')) < 1:
			return "No Name Available For This Person";
		birthNames = filter(birthNameFilter, person.get('names'));
		name = {}
		if len(birthNames) > 0:
			name = birthNames[0];
		else:
			name = person.get('names')[0]
		fullTexts = filter(fullNameFilter, name.get('nameForms'))
		return str(encode(fullTexts[0].get('fullText')))

	def getBirthInfoForPerson(person, places, full):
		def birthPlaceFilter(fact):
			return fact.get('type') == "http://gedcomx.org/Birth"
		info = {
			"date": False,
			"place": False
		}
		facts = person.get('facts')

		if facts is not None and len(facts) > 0:
			birthPlaces = filter(birthPlaceFilter, facts)
			if len(birthPlaces) > 0:
				if 'place' in birthPlaces[0]:
					print birthPlaces[0].get('place')
					info['place'] = str(encode(birthPlaces[0].get('place').get('original')))
				if 'date' in birthPlaces[0]:
					info['date'] = str(encode(birthPlaces[0].get('date').get('original')))
		if info["date"] is False or info["place"] is False:
			print "wooooo"
			print json.dumps(facts, indent=4)
			print "--------------"
			print json.dumps(places, indent=4)
			print "-------------"
			print json.dumps(full, indent=4)
		return info
	totalEntries = 0;
	totalEntriesWithDate = 0;
	totalEntriesWithOutDate = 0;
	totalEntriesWithBirthPlace = 0;
	totalEntriesWithoutBirthPlace = 0;

	#for each line in data file
	for line in fh:
		data = json.loads(line[24:])
		#persons_collection.insert_one(line[24:]);
		filteredPersons = filter(lambda x: data.get('id') == x.get('id'), data.get('persons'))
		if len(filteredPersons) > 0:
			totalEntries +=1
			person = filteredPersons[0]
			print "PERSON"
			print "ID: " + getId(person)
			print "GENDER: " + getGender(person)
			print "NAME: " + getBirthName(person)
			info = getBirthInfoForPerson(person, data.get('places'), data)
			if info["place"]:
				print "BIRTHPLACE: " + info["place"]
				totalEntriesWithBirthPlace +=1
			else:
				totalEntriesWithoutBirthPlace += 1
			if info["date"]:
				print "BIRTHDATE: " + info["date"]
				totalEntriesWithDate += 1
			else:
				totalEntriesWithOutDate += 1
	print "\n\n\n-----"
	print "STATS"
	print "total entries: " + str(totalEntries)
	print "total entries with birth date: " + str(totalEntriesWithDate)
	print "total entries without birth date: " + str(totalEntriesWithOutDate)
	print "total entries with birth place: " + str(totalEntriesWithBirthPlace)
	print "total entries without birth place: " + str(totalEntriesWithoutBirthPlace)

def insertFileToDb(fh):
	client = MongoClient('localhost', 27017)

	#get database
	db = client.family_tree_test_database;

	#get collection
	persons_collection = db.raw_persons;

	for line in fh:
		data = json.loads(line[24:].replace('.','-'))
		print( json.dumps(data, indent=4) )
		persons_collection.insert_one(data);

def findAll():
	client = MongoClient('localhost', 27017)

	#get database
	db = client.family_tree_test_database;

	#get collection
	persons_collection = db.raw_persons;
	print persons_collection.find({})


if __name__ == "__main__":
	with open(sys.argv[1]) as fh:
		findAll()
		#insertFileToDb(fh);
	# 	for a in range(0,1):

	# 	#connect to mongodb

		


