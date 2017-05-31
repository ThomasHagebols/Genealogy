from db_connect import mongo_connect
import pprint

pp = pprint.PrettyPrinter(indent=2)

def merge_people():
    mc = mongo_connect()

    for person in mc['people'].find({}):

        #start with an empty query
        query = {}

        #CheckLastName becomes None if 'PersonNameLastName' does not exist
        CheckLastName = person['PersonName'].get('PersonNameLastName')

        #If 'PersonNameLastName' exists
        if CheckLastName != None:

            #add LastName to the query
            LastName = person['PersonName']['PersonNameLastName']
            query['PersonName.PersonNameLastName'] = LastName

        #CheckFirstName becomes None if 'PersonNameFirstName' does not exist
        CheckFirstName = person['PersonName'].get('PersonNameFirstName')

        #If 'PersonNameFirstName' exists
        if CheckFirstName != None:

            #add FirstName to the query
            FirstName = person['PersonName']['PersonNameFirstName']
            query['PersonName.PersonNameFirstName'] = FirstName

        #CheckBirthDate becomes None if 'BirthDate' does not exist
        CheckBirthDate = person.get('BirthDate')

        #If 'BirthDate' exists
        if CheckBirthDate != None:
            BirthYear = person['BirthDate']['Year']
            query['BirthDate.Year'] = BirthYear

            #add BirthMonth to the query
            BirthMonth = person['BirthDate']['Month']
            query['BirthDate.Month'] = BirthMonth

            BirthDay = person['BirthDate']['Day']
            query['BirthDate.Day'] = BirthDay

        #print(query)

        #Find all the records according to the query
        results = mc['people'].find(query)

        #Empty array to store the pids of the records found by the query
        pids = []

        #Fill the 'pids' array with records found by query
        for doc in results:
            pids.append(doc['_id'])
        print(pids)

merge_people()
