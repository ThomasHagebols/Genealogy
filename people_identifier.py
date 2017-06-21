from db_connect import mongo_connect
from nltk.metrics import *
import pprint
import json
import queue
import threading


debugging = False

pp = pprint.PrettyPrinter(indent=2)
maxAllowedDistanceLevenshtein = 2

if debugging == True:
    read_table = 'people_debug'
    write_table = 'people_debug'
else:
    read_table = 'people'
    write_table = 'people'


# Calculate if a name is roughly the same using Levenshtein
def name_validation(string1, string2):
    if None in [string1, string2]:
        return False
    else:
        return True if edit_distance(string1, string2) <= maxAllowedDistanceLevenshtein else False


def check_relative_match(main_person_relations, test_person_relations):
    if None not in [main_person_relations, test_person_relations]:
        for main_rel in main_person_relations:
            for test_rel in test_person_relations:
                if None not in [main_rel.get('FirstName'), main_rel.get('LastName'), test_rel.get('FirstName'), test_rel.get('LastName')]:
                    if name_validation(main_rel.get('FirstName') + main_rel.get('LastName'),
                                       test_rel.get('FirstName') + test_rel.get('LastName')):
                        return True

        return False


def identify_people(thread_name, q):
    mc = mongo_connect()

    nr_of_jobs = 0
    subset = {'$or': [{'relatives':{'$exists': True}}, {'BirthDate': {'$exists': True}}]}
    for n, person in enumerate(mc[read_table].find(subset)):
        # start with an empty query
        query = {}

        # Get values of mandatory fields

        # CheckLastName becomes None if 'PersonNameLastName' does not exist
        LastName = person.get('PersonNameLastName')

        # CheckFirstName becomes None if 'PersonNameFirstName' does not exist
        FirstName = person.get('PersonNameFirstName')

        # CheckBirthDate becomes None if 'BirthDate' does not exist
        BirthDate = person.get('BirthDate')

        # If we don't have all the mandatory fields, we go to next loop iteration.
        # Otherwise we start to build query
        if None not in (FirstName, LastName, BirthDate):

            # --add Mandatory fields--

            # add LastName to the query
            query['PersonNameLastName'] = LastName

            # add FirstName to the query
            query['PersonNameFirstName'] = FirstName

            # print(query)
            # Find all the records according to the query
            results = mc[read_table].find(query)

            # Empty array to store the pids of the records found by the query
            work = []

            # Loop results. Add pids to pid list
            # print(person['PersonNameLastName'], person['PersonNameFirstName'], person['pid'], person['BirthDate'])
            for doc in results:
                if doc['_id'] != person['_id']:
                    # Check if birthdates match
                    if doc.get('BirthDate') == person.get('BirthDate') and None not in [doc.get('BirthDate'), person.get('BirthDate')]:
                        # TODO make birthdate more granular
                        if None not in (person['BirthDate'].get('Year'), person['BirthDate'].get('Month'), person['BirthDate'].get('Day')):
                            work.append((person['_id'], doc['_id']))
                            # print(doc['PersonNameLastName'], doc['PersonNameFirstName'], doc['pid'], doc['BirthDate'])
                    # Check if we can derive that we identified a person using relatices
                    elif check_relative_match(person.get('relatives'), doc.get('relatives')):
                        work.append((person['_id'], doc['_id']))
                        # print(doc['PersonNameLastName'], doc['PersonNameFirstName'], doc['pid'], doc['BirthDate'])
            # print('\n')
            
            # make sure that dublicates are not writen to main array
            # print(work)

            # build actual queue
            queueLock.acquire()
            for pair in work:
                workQueue.put(pair)
                nr_of_jobs +=1
            queueLock.release()

        if n % 100 == 0:
            print(n)
            print('Processing job', n, 'Length of merge list:', nr_of_jobs)


if __name__ == "__main__":
    queueLock = threading.Lock()
    workQueue = queue.Queue()

    identify_people('Identify-thread', workQueue)

