from db_connect import mongo_connect
from people_merger import name_validation
import pprint
import json
import queue as queue

debugging = False

pp = pprint.PrettyPrinter(indent=2)
maxAllowedDistanceLevenshtein = 2

#q = queue.PriorityQueue()
a=[]

if debugging == True:
    read_table = 'people_debug'
    write_table = 'people_debug'
else:
    read_table = 'people'
    write_table = 'people'

def check_relative_match(main_person_relations, test_person_relations):
    if None not in [main_person_relations, test_person_relations]:
        for main_rel in main_person_relations:
            for test_rel in test_person_relations:
                if None not in [main_rel.get('FirstName'), main_rel.get('LastName'), test_rel.get('FirstName'), test_rel.get('LastName')]:
                    if name_validation(main_rel.get('FirstName') + main_rel.get('LastName'),
                                       test_rel.get('FirstName') + test_rel.get('LastName')):
                        return True

        return False

def identify_people():
    mc = mongo_connect()

    subset = {'$or': [{'relatives':{'$exists': True}}, {'BirthDate': {'$exists': True}}]}
    for n, person in enumerate(mc[read_table].find(subset)):
        #start with an empty query
        query = {}

        #Get values of mandatory fields

        #CheckLastName becomes None if 'PersonNameLastName' does not exist
        LastName = person.get('PersonNameLastName')

        #CheckFirstName becomes None if 'PersonNameFirstName' does not exist
        FirstName = person.get('PersonNameFirstName')

        #CheckBirthDate becomes None if 'BirthDate' does not exist
        BirthDate = person.get('BirthDate')

        #If we don't have all the mandatory fields, we go to next loop iteration.
        #Otherwise we start to build query
        if None not in (FirstName, LastName, BirthDate):

            #--add Mandatory fields--

            #add LastName to the query
            query['PersonNameLastName'] = LastName

            #add FirstName to the query
            query['PersonNameFirstName'] = FirstName

            #add BirthYear to the query
            # query['BirthDate.Year'] = person['BirthDate'].get('Year')

            #add BirthMonth to the query
            # query['BirthDate.Month'] = person['BirthDate'].get('Month')

            #add Birthyear to the query
            # query['BirthDate.Day'] = person['BirthDate'].get('Day')

            # print(query)
            #Find all the records according to the query
            results = mc[read_table].find(query)

            #Empty array to store the pids of the records found by the query
            pids = []
            
            #append persons ID to first index of list
            pids.append(person['_id'])

            #Loop results. Add pids to pid list
            for doc in results:
                if doc['_id'] != person['_id']:
                    if doc.get('BirthDate') == person.get('BirthDate') and None not in [doc.get('BirthDate'), person.get('BirthDate')]:
                        pids.append(doc['_id'])
                    elif check_relative_match(person.get('relatives'), doc.get('relatives')):
                        pids.append(doc['_id'])
            
            #make sure that dublicates are not writen to main array
            if (len(pids)>1):
                #print(1.0/len(pids))
                #q.put((1.0/len(pids),pids))
                a.append(pids)

        if n%100==0:
            print(n)
            print('Length of merge list:', len(a))


if __name__ == "__main__":
    identify_people()

    with open('matches.json','w') as outfile:
        json.dump(a, outfile)

