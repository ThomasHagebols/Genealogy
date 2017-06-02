from db_connect import mongo_connect
from bson.objectid import ObjectId
from pymongo import IndexModel, ASCENDING, DESCENDING
from collections import defaultdict
import pprint
import queue
import threading
import time

debugging = False
subsample = False
sample_size = 100000

pp = pprint.PrettyPrinter(indent=2)
exitFlag = 0

# Define thread
class myThread(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
    def run(self):
        print("Starting " + self.name)
        process_collections(self.name, self.q)
        print("Exiting " + self.name)

def process_collections(threadName, q):
    while not exitFlag:
        queueLock.acquire()
        if not workQueue.empty():
            queue_item = q.get()
            queueLock.release()
            process_collection(threadName, queue_item)
        else:
            queueLock.release()
            time.sleep(1)

# This script tries to find if a person is male or female
def gender_identifier(person):
    male_indicators = ['Vader', 'Bruidegom', 'Vader van de bruidegom', 'Vader van de bruid']
    female_indicators = ['Moeder', 'Bruid','Moeder van de bruidegom', 'Moeder van de bruid']

    if person['Gender'] == "Onbekend":
        if person['RelationType'] in male_indicators:
            person['Gender'] = 'Man'
        elif person['RelationType'] in female_indicators:
            person['Gender'] = 'Vrouw'
        # For debugging purposes
        # else:
            # print('Gender unknown')
            # input("Press Enter to continue...")

    # TODO Improve gender_identifier

def analyze_person(person):
    gender_identifier(person)
    # get_approx_birthdate
    # other interesting stuff we can try to find

def get_relatives(person_main, people):
    relatives = []
    for relative in people:
        # Check if the relative and the main person are not the same
        if person_main['pid']!=relative['pid']:
            relative['Relation'] = 'No useful relation'

            # Kids and deceased have no outgoing edges
            if person_main in ['Kind', 'Overledene', 'Geregistreerde']:
                break

            # Esteblish couples
            if person_main['RelationType'] == 'Vader' and relative['RelationType'] == 'Moeder':
                relative['Relation'] = 'HasChildWith'
            if person_main['RelationType'] == 'Bruidegom' and relative['RelationType'] == 'Bruid':
                relative['Relation'] = 'MarriedTo'
            if person_main['RelationType'] == 'Vader van de bruidegom' and relative['RelationType'] == 'Moeder van de bruidegom':
                relative['Relation'] = 'HasChildWith'
            if person_main['RelationType'] == 'Vader van de bruid' and relative['RelationType'] == 'Moeder van de bruid':
                relative['Relation'] = 'HasChildWith'

            # Establish parent child relationships in marriages
            if person_main['RelationType'] == 'Vader van de bruidegom' and relative['RelationType'] == 'Bruidegom':
                relative['Relation'] = 'FatherOf'
            if person_main['RelationType'] == 'Moeder van de bruidegom' and relative['RelationType'] == 'Bruidegom':
                relative['Relation'] = 'MotherOf'
            if person_main['RelationType'] == 'Vader van de bruid' and relative['RelationType'] == 'Bruid':
                relative['Relation'] = 'FatherOf'
            if person_main['RelationType'] == 'Moeder van de bruid' and relative['RelationType'] == 'Bruid':
                relative['Relation'] = 'MotherOf'

            # Establish parent child relationships for births
            if person_main['RelationType'] == 'Vader' and relative['RelationType'] in ['Kind', 'Overledene']:
                relative['Relation'] = 'FatherOf'
            if person_main['RelationType'] == 'Moeder' and relative['RelationType'] in ['Kind', 'Overledene']:
                relative['Relation'] = 'MotherOf'

            # TODO Establish withness (Getuige) relations for  Mariage_actions and baptisms
            # TODO add "other:Eerdere vrouw", "other:Eerdere man", marriage_actions
            # TODO add husband or wife of the deceased in marriage_acts
            # TODO add 'weduwe van Willem Janssen' in deaths


            if relative['Relation'] != 'No useful relation':
                relatives.append({'pid':relative['pid'],
                                  'Relation':relative['Relation']})
            del relative['Relation']
    # Only include relatives list if it contains elements
    if relatives:
        person_main['relatives'] = relatives

def remove_people_indexes():

    try:
        mc['people'].drop_indexes()
        print("All indexes in people collection have been removed")
    except:
        print('Index not available')

def rebuild_people_indexes():

    indexes = []
    # indexes.append(IndexModel('pid', name='_pid'))
    indexes.append(IndexModel('PersonName.PersonNameLastName', name= '_LastName'))
    indexes.append(IndexModel('PersonName.PersonNameFirstName', name= '_FirstName'))
    indexes.append(IndexModel('BirthPlace', name= '_BirthPlace'))
    # indexes.append(IndexModel('BirthDate', name= '_BirthDate'))

    indexes.append(IndexModel([('BirthDate.Year', ASCENDING),
                        ('BirthDate.Month', ASCENDING),
                        ('BirthDate.Day', ASCENDING)],
                        name="_BirthDate"))

    mc['people'].create_indexes(indexes)

def save_to_db(stck, collect):
    # Try to replace the people in the table with
    try:
        collect.insert_many(stck)
        print('Batch write successful')
    except:
        for people in stack:
            del people['_id']
        mc['errors'].insert_many(stck)


# Process collections
def process_collection(thrdName, collection):
    # For all items in the current selection
    stack = []
    for n, document in enumerate(mc['source_collections'][collection].find()):
        if subsample == True and n == sample_size:
            break

        # Check if there are people in this document
        analyzed_people = []
        if 'Person' in document and 'RelationEP' in document:
            Source = {'SourceHeaderIdentifier':document['header']['identifier'], 'Collection':collection}
            if 'EventDate' in document['Event']:
                Source['EventDate'] = document['Event']['EventDate']

            analyzed_people = analyze_people(document['Person'], document['RelationEP'], Source)

        # Set pid as _id
        for analyzed_person in analyzed_people:
            if 'pid' in analyzed_person:
                analyzed_person['_id'] = analyzed_person['pid']

            stack.append(analyzed_person)

        # Once in a 100 inserts do a print statement
        if n%10000==0:
            print('Current collection:', collection, 'Opetation:', n, 'on ', thrdName)

        if debugging == True:
            print(document['header']['identifier'])

        # Write stack to db and empty the stack afterwards
        if len(stack)>20000:
            print('Writing collection:', collection, 'untill Opetation:', n, 'on', thrdName)
            save_to_db(stack, mc['people'])
            stack = []

    # Write out the rest of the stack remaining after the for loop
    if stack:
        save_to_db(stack, mc['people'])
        stack = []

def analyze_people(people, relationEP, Source):
    analyzed_people = []
    # Check how many people we have as input
    if isinstance(people,dict) and isinstance(relationEP,dict):
        # We have a single person as input
        people['RelationType'] = relationEP['RelationType']
        people['Source'] = Source
        analyze_person(people)
        # get_approx_age
        analyzed_people = [people]
    else:
        # We have multiple people or multiple relations as input
        # Convert possible dicts to lists
        if isinstance(people,dict):
            people = [people]
        if isinstance(relationEP,dict):
            relationEP = [relationEP]

        # Check if both the people list and the relationEP list have the same length
        if len(people)!=len(relationEP):
            if debugging == True:
                print(len(people), len(relationEP))
                # input("Press Enter to continue...")

            return analyzed_people

        # Make temp containing tuples of {pid:, RelationType:}
        temp = []
        for m, relation in enumerate(relationEP):
            # TODO Check if there is a better solution? Maybe ommit the join altogether?
            # Old code might be better see previous commit
            # Breaks with deaths
            if 'pid' not in people[m] and 'PersonKeyRef' in relation:
                people[m]['pid'] = relation['PersonKeyRef']
            elif 'pid' in people[m] and 'PersonKeyRef' not in relation:
                relation['PersonKeyRef'] = peopleT[m]['pid']
            elif 'pid' not in people[m] and 'PersonKeyRef' not in relation:
                # Delete person if there are no id's
                del people[m]

            temp.append({'pid':relation['PersonKeyRef'],
            'RelationType':relation['RelationType']})

        # Join temp with people
        # TODO What if pid doesn't exist?
        d = defaultdict(dict)
        for l in (people, temp):
            for elem in l:
                d[elem['pid']].update(elem)
        people = d.values()


        for person in people:
            analyze_person(person)
            get_relatives(person, people)
            person['Source'] = Source
            # TODO uncomment this thing when we have finished debugging
            # del person['RelationType']
            analyzed_people.append(person)

    return analyzed_people

if __name__ == "__main__":
    mc = mongo_connect()

    if debugging == True:
        mc['people'] = mc['people_debug']

    print("-----Do you really want to overwrite the people collection?-----")
    input("Press Enter to continue...")
    mc['people'].drop()

    # Remove indices to speed up the inserts
    remove_people_indexes()
    input("Press Enter to continue...")

    # Setup for multi-threading
    threadList = ["Thread-1", "Thread-2", "Thread-3", "Thread-4", "Thread-5", "Thread-6", "Thread-7", "Thread-8"]
    queueLock = threading.Lock()
    workQueue = queue.Queue()
    threads = []
    threadID = 1

    # Create new threads
    for tName in threadList:
        thread = myThread(threadID, tName, workQueue)
        thread.start()
        threads.append(thread)
        threadID += 1

    # Fill the queue with collections containing information
    queueLock.acquire()
    for collection in mc['source_collections']:
        workQueue.put(collection)
    queueLock.release()

    # Wait for queue to empty
    while not workQueue.empty():
        pass

    # Notify threads it's time to exit
    exitFlag = 1

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Rebuild indexes
    rebuild_people_indexes()

    print("Exiting Main Thread")
