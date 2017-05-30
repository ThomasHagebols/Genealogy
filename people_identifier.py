from db_connect import mongo_connect
import pprint

debugging = True
pp = pprint.PrettyPrinter(indent=2)

def query_person(people):
    if len(people) == 1:
        return people[0]

    return {}

def merge_person(pid1, pid2):
    mc = mongo_connect()
    read_table = 'people_debug'
    write_table = 'people_debug'

    person1 = query_person([person for person in mc[read_table].find({'_id':pid1})])
    person2 = query_person([person for person in mc[read_table].find({'_id':pid2})])

    person1keys = person1.keys()
    person2keys = person2.keys()

    both = []
    left = []
    right = []

    for key in person1keys:
        if key in person2keys:
            both.append(key)
        else:
            left.append(key)

    for key in person2keys:
        if key not in person1keys:
            right.append(key)

    print(both)
    print('\n')
    print(left)
    print('\n')
    print(right)
    print('\n')

    person_merged = {}

    # TODO Keep the most complete record
    for key in both:
        person_merged[key] = person1[key]

    for key in left:
        person_merged[key] = person1[key]

    for key in right:
        person_merged[key] = person2[key]

    # Check for dangling links!!!
    for peopleWithDangingLinks in mc[read_table].find({'relatives.pid':pid2}):
        for relatives in peopleWithDangingLinks:
            if relatives['pid'] == pid2:
                relatives['pid'] = pid1

    # Still need to implement the overwrite function for the new person AND the danglink links
    # if debugging ==True:
        # Save to people_debug


if __name__ == "__main__":
    # pid1 = 'Person:2eaf362c-4630-11e3-a747-d206bceb4d38'
    pid1 = 'Person:68a683c0-4631-11e3-a747-d206bceb4d38'
    pid2 = 'Person:68a683c0-4631-11e3-a747-d206bceb4d38'

    merge_person(pid1, pid2)
