from db_connect import mongo_connect
import itertools
import pprint

debugging = True
pp = pprint.PrettyPrinter(indent=2)

if debugging == True:
    read_table = 'people'
    write_table = 'people'
else:
    read_table = 'people'
    write_table = 'people'

def relation_checker(relatives):
    mc = mongo_connect()

    relations_with_multiple_pids = []
    for i in itertools.product(relatives, relatives):
        if i[0]['Relation'] == i[1]['Relation'] and i[0]['pid'] != i[1]['pid']:
            pid_pair = [i[0]['pid'], i[1]['pid']]
            pid_pair.sort()
            pid_pair = tuple(pid_pair)
            relations_with_multiple_pids.append(pid_pair)

    relations_with_multiple_pids = list(set(relations_with_multiple_pids))

    # Convert list of tuples to list of lists
    temp = []
    for i in relations_with_multiple_pids:
        temp.append(list(i))
        print(type(i))
    relations_with_multiple_pids = temp

    print(relations_with_multiple_pids)

    relatives_with_multiple_pids = []
    for pids in relations_with_multiple_pids:
        person1 = mc[read_table].find_one({'_id':pids[0]})
        person2 = mc[read_table].find_one({'_id':pids[1]})

        if person1['PersonName']['PersonNameLastName'] == person2['PersonName']['PersonNameLastName'] and person1['PersonName']['PersonNameFirstName'] == person2['PersonName']['PersonNameFirstName']:
            relatives_with_multiple_pids.append(pids)

        print(person1)
        print(person2)

    return relatives_with_multiple_pids

def remove_links_to_old_pid(pid1, pid2):
    mc = mongo_connect()

    for personWithDangingLinks in mc[read_table].find({'relatives.pid':pid2}):
        for relative in personWithDangingLinks['relatives']:
            if relative['pid'] == pid2:
                relative['pid'] = pid1

            print(personWithDangingLinks['relatives'])

            mc[write_table].find_one_and_update({'_id':personWithDangingLinks['_id']},
                                                {'$set':{'relatives':personWithDangingLinks['relatives']}})

def merge_person(pid1, pid2):
    mc = mongo_connect()

    # Get people from the database
    person1 = mc[read_table].find_one({'_id':pid1})
    person2 = mc[read_table].find_one({'_id':pid2})

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

    # Print some stuff when debugging is true
    if debugging == True:
        pp.pprint(person1)
        pp.pprint(person2)
        print('\n')
        print(both)
        print('\n')
        print(left)
        print('\n')
        print(right)
        print('\n')

    # TODO Keep the most complete record
    person_merged = {}
    for key in both:
        if key == 'Source':
            person_merged[key] = [person1[key], person2[key]]
        elif key == 'Gender':
            if person1[key] == 'Onbekend':
                person_merged[key] = person2[key]
            else:
                person_merged[key] = person1[key]
        elif key == 'relatives':
            person_merged[key] = []
            # TODO Check for double links after merge

            for relative in person1[key]:
                person_merged[key].append(relative)

            for relative in person2[key]:
                if relative not in person_merged[key]:
                    person_merged[key].append(relative)
        elif key == 'pid':
            person_merged[key] = person1[key]
        else:
            if debugging == True:
                print(person1[key], person2[key])
            person_merged[key] = person1[key]

    for key in left:
        person_merged[key] = person1[key]

    for key in right:
        person_merged[key] = person2[key]

    pp.pprint(person_merged)

    # # Check for dangling links!!!
    remove_links_to_old_pid(pid1, pid2)

    # mc[write_table].find_one_and_replace({'_id':pid1}, person_merged)
    # mc[write_table].find_one_and_delete({'_id':pid2})

    # Find if there are relations with multiple pid's and check if they are the same person
    relatives_with_multiple_pids = relation_checker(person_merged['relatives'])

    # Recurse merge_person
    for relative in relatives_with_multiple_pids:
        merge_person(relative[0], relative[1])


if __name__ == "__main__":
    personID1 = 'Person:2eaf362c-4630-11e3-a747-d206bceb4d38'
    # pid1 = 'Person:68a683c0-4631-11e3-a747-d206bceb4d38'
    personID2 = 'Person:68a683c0-4631-11e3-a747-d206bceb4d38'

    merge_person(personID1, personID2)
