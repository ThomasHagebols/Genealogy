from db_connect import mongo_connect
from nltk.metrics import *
from datetime import datetime
import pandas as pd
import itertools
import random
import queue
import threading
import time
import pprint


debugging = False
dry_run = False
short_q = False

read_table = 'people'
write_table = 'people'

pp = pprint.PrettyPrinter(indent=2)

exitflag = False
identifierFinished = False
mc = mongo_connect()

if short_q:
    min_qsize = 1
else:
    min_qsize = 2000


class IdentifyThread (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        print("Starting " + self.name)
        identify_people()
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
    count = 0
    while not exitflag:
        queueLock.acquire()
        if not workQueue.empty() and (q.qsize() > min_qsize or identifierFinished):
            q_item = q.get()
            queueLock.release()

            # Split merging of multiple documents in pairwise merges
            work = split_job(q_item[1])

            for job in work:
                try:
                    print(thread_name, 'Merge:', count, 'Queue size:', q.qsize())
                    merge_person(thread_name, job[0], job[1])
                    count += 1
                except Exception as e:
                    queueLock.acquire()
                    q.put((random.random(), [job[0], job[1]]))
                    queueLock.release()
                    time.sleep(1)
                    print('error with ', job)
                    print(e)
        else:
            queueLock.release()
        time.sleep(0.05)


def split_job(queue_job):
    work = []
    while len(queue_job) > 1:
        work.append((queue_job[0], queue_job[1]))
        del queue_job[1]

    return work


# Calculate if a name is roughly the same using Levenshtein
def name_validation(p1First, p1Last, p2First,p2Last, maxAllowedDistanceLevenshtein):
    if None in [p1First, p1Last, p2First,p2Last]:
        return False
    else:
        return True if edit_distance(p1First + p1Last, p2First + p2Last) <= maxAllowedDistanceLevenshtein else False


# Match on name and relation type
def check_relative_match(main_person_relations, test_person_relations):
    if None not in [main_person_relations, test_person_relations]:
        for main_rel in main_person_relations:
            for test_rel in test_person_relations:
                if None not in [main_rel.get('FirstName'), main_rel.get('LastName'), test_rel.get('FirstName'),
                                test_rel.get('LastName')]:
                    if main_rel.get('Relation') == test_rel.get('Relation') and None not in [main_rel.get('Relation'), test_rel.get('Relation')]:
                        if name_validation(main_rel.get('FirstName'), main_rel.get('LastName'),
                                           test_rel.get('FirstName'), test_rel.get('LastName'), 1):
                            return True

    return False


def identify_people():
    global identifierFinished

    nr_of_jobs = 0
    subset = {'$or': [{'relatives': {'$exists': True}}, {'BirthDate': {'$exists': True}}]}
    for n, person in enumerate(mc[read_table].find(subset).batch_size(100)):
        # start with an empty query
        query = {}

        LastName = person.get('PersonNameLastName')
        FirstName = person.get('PersonNameFirstName')

        if None not in (FirstName, LastName):
            query['PersonNameLastName'] = LastName
            query['PersonNameFirstName'] = FirstName

            # Find all the records according to the query
            results = mc[read_table].find(query)

            # Empty array to store the pids of the records found by the query
            work = [person['_id']]

            # Loop results. Add pids to pid list
            # print(person['PersonNameLastName'], person['PersonNameFirstName'], person['pid'], person['BirthDate'])
            for doc in results:
                if doc['_id'] != person['_id']:
                    # Check if birthdates are present
                    if None not in [doc.get('BirthDate'), person.get('BirthDate')]:
                        # Check if birthdate is the same and there are no missing values of the date field
                        if doc.get('BirthDate') == person.get('BirthDate') and {'Year', 'Month', 'Day'} <= set(person['BirthDate'].keys()):
                            work.append(doc['_id'])
                            # print(doc['PersonNameLastName'], doc['PersonNameFirstName'], doc['pid'], doc['BirthDate'])
                    elif check_relative_match(person.get('relatives'), doc.get('relatives')):
                        work.append(doc['_id'])
                        # print(doc['PersonNameLastName'], doc['PersonNameFirstName'], doc['pid'], doc['BirthDate'])
            # print('\n')

            # build actual queue
            queueLock.acquire()
            if len(work) > 1:
                workQueue.put((random.random(), work))
                nr_of_jobs += 1
            queueLock.release()

        if n % 100 == 0:
            print('Currently identifying doc', n, 'Length of merge list:', nr_of_jobs)

    identifierFinished = True


def remove_duplicates_in_relatives_on_pid(relatives):
    # Convert date to int (workaround for out odf bounds error)
    for relative in relatives:
        relative['DateTo'] = int(datetime.strftime(relative['DateTo'],'%Y%m%d'))
        relative['DateFrom'] = int(datetime.strftime(relative['DateFrom'],'%Y%m%d'))

    relatives_df = pd.DataFrame.from_dict(relatives)

    if 'FirstName' in list(relatives_df) and 'LastName' in list(relatives_df):
        max_val = relatives_df.groupby(['pid'], as_index=False).max()[['pid', 'Relation', 'temporaryRelation',
                                                                       'FirstName', 'LastName', 'DateTo']]
    elif 'FirstName' in list(relatives_df):
        max_val = relatives_df.groupby(['pid'], as_index=False).max()[['pid', 'Relation', 'temporaryRelation',
                                                                       'FirstName', 'DateTo']]
    elif 'LastName' in list(relatives_df):
        max_val = relatives_df.groupby(['pid'], as_index=False).max()[['pid', 'Relation', 'temporaryRelation',
                                                                       'LastName', 'DateTo']]
    else:
        max_val = relatives_df.groupby(['pid'], as_index=False).max()[['pid', 'Relation', 'temporaryRelation', 'DateTo']]

    min_val = relatives_df.groupby(['pid'], as_index=False).min()[['pid', 'DateFrom']]

    relatives_df = pd.merge(max_val, min_val, on='pid')

    result = relatives_df.to_dict(orient='records')

    # Convert int to date
    for relative in result:
        relative['DateTo'] = datetime.strptime(str(relative['DateTo']),'%Y%m%d')
        relative['DateFrom'] = datetime.strptime(str(relative['DateFrom']),'%Y%m%d')
        if type(relative.get('FirstName')) == float:
            del relative['FirstName']
        if type(relative.get('LastName')) == float:
            del relative['LastName']

    return result


def remove_links_to_old_pid(blk, pid1, pid2):
    for personWithDangingLinks in mc[read_table].find({'relatives.pid': pid2}):
        for relative in personWithDangingLinks['relatives']:
            if relative['pid'] == pid2:
                relative['pid'] = pid1

                # print(personWithDangingLinks['relatives'])

        # Remove duplicates
        personWithDangingLinks['relatives'] = remove_duplicates_in_relatives_on_pid(personWithDangingLinks['relatives'])

        # TODO Check if this works correctly
        personWithDangingLinks['relatives'] = [dict(tpl) for tpl in
                                               set([tuple(dct.items()) for dct in personWithDangingLinks['relatives']])]

        blk.find({'_id': personWithDangingLinks['_id']}).update(
            {'$set': {'relatives': personWithDangingLinks['relatives']}})


def is_married_parent(role1, role2):
    return True if role1 in ['HasChildWith', 'MarriedTo'] and role2 in ['HasChildWith', 'MarriedTo'] else False


def relation_checker(relatives):
    relations_with_multiple_pids = []
    # Do a cartesian product where relation[0] == relation[1] and pid[0[!=pid[1]
    for i in itertools.product(relatives, relatives):
        if (i[0]['Relation'] == i[1]['Relation'] or is_married_parent(i[0]['Relation'], i[1]['Relation'])) and i[0]['pid'] != i[1]['pid']:
            pid_pair = [i[0]['pid'], i[1]['pid']]
            pid_pair.sort()
            pid_pair = tuple(pid_pair)
            relations_with_multiple_pids.append((pid_pair, i[0]['Relation']))

    # Build dictionary for easy lookup of relatives by pid
    relative_dict = {}
    for person in relatives:
        temp = {}

        if person.get('FirstName'):
            temp['FirstName'] = person.get('FirstName')

        if person.get('LastName'):
            temp['LastName'] = person.get('LastName')

        relative_dict[person['pid']] = temp


    # remove duplicates in pid pairs
    relations_with_multiple_pids = list(set(relations_with_multiple_pids))

    relatives_with_multiple_pids = []
    for pids_relation in relations_with_multiple_pids:
        # A person can only have a single father and mother. Hence always merge them
        if pids_relation[1] in ['FatherOf', 'MotherOf']:
            relatives_with_multiple_pids.append(pids_relation)
        else:
            name_match = name_validation(relative_dict[pids_relation[0][0]].get('FirstName'),
                                         relative_dict[pids_relation[0][0]].get('LastName'),
                                         relative_dict[pids_relation[0][1]].get('FirstName'),
                                         relative_dict[pids_relation[0][1]].get('LastName'),2)

            if name_match:
                relatives_with_multiple_pids.append(pids_relation)
                if debugging:
                    print('Match:', pids_relation[0][0],
                          relative_dict[pids_relation[0][0]].get('FirstName'),
                          relative_dict[pids_relation[0][0]].get('LastName'))
                    print('Match:', pids_relation[0][1],
                          relative_dict[pids_relation[0][1]].get('FirstName'),
                          relative_dict[pids_relation[0][1]].get('LastName'))
            else:
                if debugging:
                    print('No match:', pids_relation[0][0],
                          relative_dict[pids_relation[0][0]].get('FirstName'),
                          relative_dict[pids_relation[0][0]].get('LastName'))
                    print('No match:', pids_relation[0][1],
                          relative_dict[pids_relation[0][1]].get('FirstName'),
                          relative_dict[pids_relation[0][1]].get('LastName'))
    return relatives, relatives_with_multiple_pids


def merge_person(t_name, pid1, pid2):
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

    # Remove duplicates on pid
    if person_merged.get('relatives'):
        person_merged['relatives'] = remove_duplicates_in_relatives_on_pid(person_merged['relatives'])

    # remove duplicates on name and get merge list of these duplicates
        person_merged['relatives'], relatives_with_multiple_pids = relation_checker(person_merged['relatives'])

    # Check for dangling links!!!
    remove_links_to_old_pid(bulk, pid1, pid2)

    bulk.find({'_id': pid1}).replace_one(person_merged)
    bulk.find({'_id': pid2}).remove()

    if not dry_run:
        bulk.execute()

    # Recurse merge_person
    if person_merged.get('relatives'):
        print(relatives_with_multiple_pids)
        for relative in relatives_with_multiple_pids:
            try:
                merge_person(t_name, relative[0][0], relative[0][1])
            except Exception as e:
                print('error with recursive job', relative[0][0], relative[0][1])
                print(e)


if __name__ == "__main__":
    if debugging:
        threadList = ['Thread-' + str(i) for i in range(0, 1)]
    else:
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
