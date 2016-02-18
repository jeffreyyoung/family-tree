from __future__ import print_function

import sys
from operator import add
import dateutil.parser as dateParser
from pyspark import SparkContext
import json


class FACT_TYPES:

    BIRTH_FACT = 'http://gedcomx.org/Birth'
    CHRISTENING_FACT = 'http://gedcomx.org/Christening'
    DEATH_FACT = 'http://gedcomx.org/Death'
    BURIAL_FACT = 'http://gedcomx.org/Burial'
    RESIDENCE_FACT = 'http://gedcomx.org/Residence'


class RELATIONSHIP_TYPES:

    PARENT_CHILD = 'http://gedcomx.org/ParentChild'
    COUPLE = 'http://gedcomx.org/Couple'


# takes as input a person json file
# outputs the range in which the person was alive, and the location of where the person lived

def get_person_info(line):

    def encode(text):
        try:
            return text.decode('utf-8')
        except:
            return "encode error"

    def getId(person):
        return str(person.get('id'))

    def getPerson(person_id, persons):
        filteredPersons = filter(lambda x: data.get('id') == x.get('id'), persons)
        if len(filteredPersons) > 0:
            return filteredPersons[0]
        else:
            return False

    def getFact(facts, type):
        filteredFacts = filter(lambda f: f.get('type') == type, facts)
        if len(filteredFacts) > 0:
            return filteredFacts[0]
        else:
            return False

    def getDateAndPlace(fact):
        info = {'date': False, 'place': False}
        if fact is not False:
            if 'place' in fact:
                if 'normalized' in fact.get('place'):
                    info['place'] = str(fact.get('place').get('normalized')[0].get('value'))
                else:
                    info['place'] = str(encode(fact.get('place').get('original')))
            if 'date' in fact:
                if 'normalized' in fact.get('date'):
                    info['date'] = str(fact.get('date').get('normalized')[0].get('value'))
                else:
                    info['date'] = str(encode(fact.get('date').get('original')))
        return info

    def getFactInfo(person, places, fact_type):
        info = {'date': False, 'place': False}
        facts = person.get('facts')
        if facts is None or len(facts) <= 0:
            return info
        fact = getFact(person.get('facts'), fact_type)
        return getDateAndPlace(fact)

    def extractYearFromDate(data):
        try:
            data = str(data)
            return str(dateParser.parse(data).year)
        except:
            return data
    def extractCountryFromPlace(place):
        try:
            places = place.split(',')
            return places[ len(places) - 1].strip()
        except:
            return place
    person_id = line[:23]
    data = json.loads(line[24:])
    persons = data.get('persons')
    places = data.get('places')
    relationships = data.get('relationships')
    person = getPerson(person_id, persons)
    info = getFactInfo(person, places, FACT_TYPES.DEATH_FACT)
    return encode(extractYearFromDate(str(info['date']))) + "::" + encode(str(extractCountryFromPlace(info['place'])))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: wordcount <file>', file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName='PythonWordCount')
    lines = sc.textFile(sys.argv[1], 1)
    counts = lines.map(lambda x: (get_person_info(x), 1))\
        .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print('%s: %i' % (word, count))

    sc.stop()
