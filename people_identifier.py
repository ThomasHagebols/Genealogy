from db_connect import mongo_connect
from bson.objectid import ObjectId
from pymongo import IndexModel
from collections import defaultdict
import pprint

pp = pprint.PrettyPrinter(indent=2)

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
    for person in people:
        if person_main['pid']!=person['pid']:
            # Here there should be some code which determines the reletives and their connections
            # Right now we only have edges without edge labels
            relatives.append({'pid':person['pid']})
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
    if isinstance(people,dict):
        # We have a single person as input
        people['RelationType'] = relationEP['RelationType']
        people['Source'] = Source
        analyze_person(people)
        # get_approx_age
        analyzed_people = [people]
    else:
        # We have multiple people as input
        # Check if both the people list and the relationEP list have the same length
        if len(people)!=len(relationEP):
            print(len(people), len(relationEP))
            input("Press Enter to continue...")

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
                relation['PersonKeyRef'] = people[n]['pid']
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
            analyzed_people.append(person)

    return analyzed_people

if __name__ == "__main__":
    client, bhic, source_collections, people = mongo_connect()

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
        print(collection)
        for n, document in enumerate(source_collections[collection].find()):
            print(document['_id'])

            # Check if there are people in this document
            analyzed_people = []
            if 'Person' in document and 'RelationEP' in document:
                Source = {'SourceId':document['_id'], 'Collection':collection}
                if 'EventDate' in document['Event']:
                    Source['EventDate'] = document['Event']['EventDate']

                analyzed_people = analyze_people(document['Person'], document['RelationEP'], Source)

            for analyzed_person in analyzed_people:
                if 'pid' in analyzed_person:
                    analyzed_person['_id'] = analyzed_person['pid']
                # Use insert_one for easier debugging
                people.insert_one(analyzed_person)
            # people.insert_many(analyzed_people)
            if n%20==0:
                print(collection)
                print('Current collection:', collection, 'Opetation:', n)

    # Rebuild indexes
    rebuild_people_indexes()
