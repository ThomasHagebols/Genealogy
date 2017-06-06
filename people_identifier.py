from db_connect import mongo_connect
import pickle
import people_merger
import pprint
import queue as queue

debugging = True

output = open('data.pkl', 'wb')
pp = pprint.PrettyPrinter(indent=2)
q = queue.PriorityQueue()

if debugging == True:
    read_table = 'people_debug'
    write_table = 'people_debug'
else:
    read_table = 'people'
    write_table = 'people'

def identify_people():
    mc = mongo_connect()

    for n, person in enumerate(mc[read_table].find({})):
        if n%100000==0:
            print(n)

        if n > 1000000:
            break

        #start with an empty query
        query = {}

        #Get values of mandatory fields

        #CheckLastName becomes None if 'PersonNameLastName' does not exist
        LastName = person.get('PersonNameLastName')

        #CheckFirstName becomes None if 'PersonNameFirstName' does not exist
        FirstName = person.get('PersonNameFirstName')

        #CheckBirthDate becomes None if 'BirthDate' does not exist
        BirthDate = person.get('BirthDate')

        #If we don't have all the mandatory fields, we go to next loop iteration. Otherwise we start to build query
        if None not in (FirstName, LastName, BirthDate):


            #--add Mandatory fields--

            #add LastName to the query
            query['PersonNameLastName'] = LastName

            #add FirstName to the query
            query['PersonNameFirstName'] = FirstName

            #add BirthYear to the query
            query['BirthDate.Year'] = person['BirthDate'].get('Year')

            #add BirthMonth to the query
            query['BirthDate.Month'] = person['BirthDate'].get('Month')

            #add Birthyear to the query
            query['BirthDate.Day'] = person['BirthDate'].get('Day')


            #Find optional fields
            optional = {}


            if person.get('PersonNamePrefixLastName') != None:
                optional['PersonNamePrefixLastName'] = person['PersonNamePrefixLastName']

            if person.get('BirthPlace') != None:
                optional['BirthPlace.Place'] = person['BirthPlace'].get('Place')

            if person.get('Residence') != None:
                optional['Residence.Place'] = person['Residence'].get('Place')

            if person.get('PersonNamePatronym') != None:
                optional['PersonNamePatronym'] = person.get('PersonNamePatronym')


            #Find all the records according to the query
            results = mc[read_table].find(query)

            #Empty array to store the pids of the records found by the query
            pids = []

            #Empty array to store the scores of the records
            scores = []

            #Loop results
            for doc in results:
                score = 0
                #Check if any optional fields match. Give score for matches
                for key, value in optional.items():
                    if doc.get(key) == value:
                        score+=1
                if doc['_id'] != person['_id']:
                    pids.append(doc['_id'])
                    scores.append(score)

            #print(pids, scores)
            # scored_pid_pair = []
            for p,s in zip(pids,scores):
                q.put([s,person.get('_id'),p])
                if (s>0):
                    print(person.get('_id'),pids, scores)

            # pickle.dumps(scored_pid_pair, output)

if __name__ == "__main__":
    identify_people()
