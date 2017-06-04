from db_connect import mongo_connect
import pprint

debugging = True
pp = pprint.PrettyPrinter(indent=2)

if debugging == True:
    read_table = 'people'
    write_table = 'people_debug'
else:
    read_table = 'people'
    write_table = 'people'

def query_person(people):
    if len(people) == 1:
        return people[0]

    return {}

def merge_person(pid1, pid2):
    mc = mongo_connect()

    # Get people from the database
    person1 = query_person([person for person in mc[read_table].find({'_id':pid1})])
    person2 = query_person([person for person in mc[read_table].find({'_id':pid2})])

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
        print(person1)
        print(person2)
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
    for peopleWithDangingLinks in mc[read_table].find({'relatives.pid':pid2}):
        for relative in peopleWithDangingLinks['relatives']:
            print(relative)
            # TODO Possible fancy recursive function
            # if relatives['pid'] == pid2:
            #     relatives['pid'] = pid1

    # Still need to implement the overwrite function for the new person AND the danglink links
    # if debugging ==True:
        # Save to people_debug


if __name__ == "__main__":
    pid1 = 'Person:2eaf362c-4630-11e3-a747-d206bceb4d38'
    # pid1 = 'Person:68a683c0-4631-11e3-a747-d206bceb4d38'
    pid2 = 'Person:68a683c0-4631-11e3-a747-d206bceb4d38'

    merge_person(pid1, pid2)
