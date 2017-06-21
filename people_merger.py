from db_connect import mongo_connect
from nltk.metrics import *
from datetime import datetime
import pandas as pd
import random
import queue
import threading
import time
import itertools
import pprint

# TODO build lock!!!

debugging = False
dry_run = False
read_table = 'people'
write_table = 'people'
maxAllowedDistanceLevenshtein = 2

pp = pprint.PrettyPrinter(indent=2)

count = 0

exitflag = False
identifierFinished = False
mc = mongo_connect()

class IdentifyThread (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        print("Starting " + self.name)
        identify_people(self.name, self.q)
        print("Exiting " + self.name)


class MergeThread (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        print("Starting " + self.name)
        process_data(self.name, self.q)
        print("Exiting " + self.name)


def process_data(thread_name, q):
    global count
    while not exitflag:
        queueLock.acquire()
        # TODO fix queue collision problem in a better way than we did here. Dirty hack
        # Queue can be jobs with [pid1, pid2, pid3] and let the thread split up the work
        if not workQueue.empty() and (q.qsize() > 5000 or identifierFinished):
            q_item = q.get()
            queueLock.release()
            try:
                print(thread_name, 'Merge:', count, 'Queue size:', q.qsize())
                merge_person(q, thread_name, q_item[1][0], q_item[1][1])
                count += 1
            except Exception as e:
                queueLock.acquire()
                q.put((random.random(), (q_item[1][0], q_item[1][1])))
                queueLock.release()
                time.sleep(1)
                print('error with ', q_item[1])
                print(e)
        else:
            queueLock.release()
        time.sleep(0.05)


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
                if None not in [main_rel.get('FirstName'), main_rel.get('LastName'), test_rel.get('FirstName'),
                                test_rel.get('LastName')]:
                    if name_validation(str(main_rel.get('FirstName')) + str(main_rel.get('LastName')),
                                       str(test_rel.get('FirstName')) + str(test_rel.get('LastName'))):
                        return True

        return False


def identify_people(thread_name, q):
    global identifierFinished
    mc = mongo_connect()

    nr_of_jobs = 0
    subset = {'$or': [{'relatives': {'$exists': True}}, {'BirthDate': {'$exists': True}}]}
    for n, person in enumerate(mc[read_table].find(subset).batch_size(100)):
        # start with an empty query
        query = {}

        LastName = person.get('PersonNameLastName')
        FirstName = person.get('PersonNameFirstName')

        if None not in (FirstName, LastName):
            query['PersonNameLastName'] = person.get('PersonNameLastName')
            query['PersonNameFirstName'] = person.get('PersonNameFirstName')

            # Find all the records according to the query
            results = mc[read_table].find(query)

            # Empty array to store the pids of the records found by the query
            work = []

            # Loop results. Add pids to pid list
            # print(person['PersonNameLastName'], person['PersonNameFirstName'], person['pid'], person['BirthDate'])
            for doc in results:
                if doc['_id'] != person['_id']:
                    # Check if birthdates match
                    if doc.get('BirthDate') == person.get('BirthDate') and None not in [doc.get('BirthDate'),
                                                                                        person.get('BirthDate')]:
                        if None not in (person['BirthDate'].get('Year'), person['BirthDate'].get('Month'),
                                        person['BirthDate'].get('Day')):
                            work.append((person['_id'], doc['_id']))
                            # print(doc['PersonNameLastName'], doc['PersonNameFirstName'], doc['pid'], doc['BirthDate'])

                    # Check if we can derive that we identified a person using relatives
                    elif check_relative_match(person.get('relatives'), doc.get('relatives')):
                        work.append((person['_id'], doc['_id']))
                        # print(doc['PersonNameLastName'], doc['PersonNameFirstName'], doc['pid'], doc['BirthDate'])
            # print('\n')

            # build actual queue
            queueLock.acquire()
            for pair in work:
                # print('Adding to queue:', pair)
                workQueue.put((random.random(), pair))
                nr_of_jobs += 1
            queueLock.release()

        if n % 100 == 0:
            print('Currently identifying doc', n, 'Length of merge list:', nr_of_jobs)

    identifierFinished = True


def remove_duplicates_in_relatives(relatives):
    # Convert date to int (workaround for out odf bounds error)
    for relative in relatives:
        relative['DateTo'] = int(datetime.strftime(relative['DateTo'],'%Y%m%d'))
        relative['DateFrom'] = int(datetime.strftime(relative['DateFrom'],'%Y%m%d'))

    relatives_df = pd.DataFrame.from_dict(relatives)

    max_val = relatives_df.groupby(['pid'], as_index=False).max()[['pid', 'Relation', 'temporaryRelation', 'FirstName',
                                                                   'LastName', 'DateTo']]
    min_val = relatives_df.groupby(['pid'], as_index=False).min()[['pid', 'DateFrom']]

    relatives_df = pd.merge(max_val, min_val, on='pid')

    result = relatives_df.to_dict(orient='records')

    # Convert int to date
    for relative in result:
        relative['DateTo'] = datetime.strptime(str(relative['DateTo']),'%Y%m%d')
        relative['DateFrom'] = datetime.strptime(str(relative['DateFrom']),'%Y%m%d')

    return result


def remove_links_to_old_pid(blk, pid1, pid2):
    for personWithDangingLinks in mc[read_table].find({'relatives.pid': pid2}):
        for relative in personWithDangingLinks['relatives']:
            if relative['pid'] == pid2:
                relative['pid'] = pid1

                # print(personWithDangingLinks['relatives'])

        # Remove duplicates
        personWithDangingLinks['relatives'] = remove_duplicates_in_relatives(personWithDangingLinks['relatives'])

        # TODO Check if this works correctly
        personWithDangingLinks['relatives'] = [dict(tpl) for tpl in
                                               set([tuple(dct.items()) for dct in personWithDangingLinks['relatives']])]

        blk.find({'_id': personWithDangingLinks['_id']}).update(
            {'$set': {'relatives': personWithDangingLinks['relatives']}})


def merge_person(q, t_name, pid1, pid2):
    bulk = mc[write_table].initialize_ordered_bulk_op()

    # Get people from the database
    print(t_name, 'Merging:', pid1, pid2)
    person1 = mc[read_table].find_one({'_id': pid1})
    person2 = mc[read_table].find_one({'_id': pid2})

    # If person1 or person2 are empty return
    if person1 is None or person2 is None:
        return

    # Check where the information is contained person1 ->left, person2 -> right
    both = []
    left = []
    right = []

    for key in person1.keys():
        if key in person2.keys():
            both.append(key)
        else:
            left.append(key)

    for key in person2.keys():
        if key not in person1.keys():
            right.append(key)

    # TODO Keep the most complete record
    person_merged = {}
    for key in both:
        if key == 'Sources':
            person_merged[key] = []
            for source in person1[key]:
                person_merged[key].append(source)
            for source in person2[key]:
                person_merged[key].append(source)
        elif key == 'Gender':
            if person1[key] == 'Onbekend':
                person_merged[key] = person2[key]
            else:
                person_merged[key] = person1[key]
        elif key == 'relatives':
            person_merged[key] = []
            for relative in person1[key]:
                person_merged[key].append(relative)

            for relative in person2[key]:
                if relative not in person_merged[key]:
                    person_merged[key].append(relative)

            person_merged[key] = remove_duplicates_in_relatives(person_merged[key])
        elif key == 'pid':
            person_merged[key] = person1[key]
        else:
            # if debugging:
            #     print(person1[key], person2[key])
            person_merged[key] = person1[key]

    for key in left:
        person_merged[key] = person1[key]

    for key in right:
        person_merged[key] = person2[key]

    # Check for dangling links!!!
    remove_links_to_old_pid(bulk, pid1, pid2)

    bulk.find({'_id': pid1}).replace_one(person_merged)
    bulk.find({'_id': pid2}).remove()

    if not dry_run:
        bulk.execute()


if __name__ == "__main__":
    threadList = ['Thread-' + str(i) for i in range(0, 10)]

    queueLock = threading.Lock()
    workQueue = queue.PriorityQueue()
    threads = []
    threadID = 1

    # Create new threads
    thread = IdentifyThread(threadID, 'Identify-thread', workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1

    # Create merge thread
    for tName in threadList:
        thread = MergeThread(threadID, tName, workQueue)
        thread.start()
        threads.append(thread)
        threadID += 1

    # Wait for queue to empty
    while not identifierFinished or not workQueue.empty():
        time.sleep(10)

    # Notify threads it's time to exit
    exitflag = True
