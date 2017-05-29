from db_connect import mongo_connect
from bson.objectid import ObjectId
from pymongo import IndexModel
from collections import defaultdict
import pprint

pp = pprint.PrettyPrinter(indent=2)
subsample = False
sample_size = 10

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

            if relative['Relation'] != 'No useful relation':
                relatives.append({'pid':relative['pid'],
                                  'Relation':relative['Relation']})
            del relative['Relation']
    # Only include relatives list if it contains elements
    if relatives:
        person_main['relatives'] = relatives

def remove_people_indexes():
    try:
        people.drop_indexes()
    except:
        print('Index not available')
    print("All documents in people collection have been removed")

def rebuild_people_indexes():
    index_pid = IndexModel('pid', name='_pid')
    index_lastname = IndexModel('PersonName.PersonNameLastName', name= '_LastName')
    people.create_indexes([index_pid, index_lastname])


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
            print(len(people), len(relationEP))
            # input("Press Enter to continue...")

            return analyzed_people

        # Make temp containing tuples of {pid:, RelationType:}
        temp = []
        for n, relation in enumerate(relationEP):
            # TODO Check if there is a better solution? Maybe ommit the join altogether?
            # Old code might be better see previous commit
            # Breaks with deaths
            if 'pid' not in people[n] and 'PersonKeyRef' in relation:
                people[n]['pid'] = relation['PersonKeyRef']
            elif 'pid' in people[n] and 'PersonKeyRef' not in relation:
                relation['PersonKeyRef'] = peopleT[n]['pid']
            elif 'pid' not in people[n] and 'PersonKeyRef' not in relation:
                # Delete person if there are no id's
                del people[n]

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
    client, bhic, source_collections, people, errors = mongo_connect()

    print("-----Do you really want to overwrite the people collection?-----")
    input("Press Enter to continue...")
    people.remove()

    # Remove indices to speed up the inserts
    remove_people_indexes()
    input("Press Enter to continue...")

    # TODO Low prio make loop multi threaded
    # Build stack containing collections
    # run multiple collections in parrallel

    # Loop over all collections containing information
    for collection in source_collections:
        # For all items in the current selection
        for n, document in enumerate(source_collections[collection].find()):
            if subsample == True and n == sample_size:
                break

            if n%20==0:
                print(collection)
                print('Current collection:', collection, 'Opetation:', n)

            print(document['header']['identifier'])

            # Check if there are people in this document
            analyzed_people = []
            if 'Person' in document and 'RelationEP' in document:
                Source = {'SourceHeaderIdentifier':document['header']['identifier'], 'Collection':collection}
                if 'EventDate' in document['Event']:
                    Source['EventDate'] = document['Event']['EventDate']

                analyzed_people = analyze_people(document['Person'], document['RelationEP'], Source)

            for analyzed_person in analyzed_people:
                try:
                    if 'pid' in analyzed_person:
                        analyzed_person['_id'] = analyzed_person['pid']
                    # Use insert_one for easier debugging
                    people.insert_one(analyzed_person)
                except:
                    del analyzed_person['_id']
                    people.insert_one(analyzed_person)
            # people.insert_many(analyzed_people)


    # Rebuild indexes
    rebuild_people_indexes()
