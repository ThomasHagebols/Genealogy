from db_connect import mongo_connect
import pprint

pp = pprint.PrettyPrinter(indent=4)

def gender_identifier(person, relation):
    male_indicators = ['Vader', 'Bruidegom', 'Vader van de bruidegom', 'Vader van de bruid']
    female_indicators = ['Moeder', 'Bruid','Moeder van de bruidegom', 'Moeder van de bruid']

    if person['Gender'] == "Onbekend":
        # print("Gender unknown")

        if relation['RelationType'] in male_indicators:
            person['Gender'] = 'Man'
            # print('Set man')
        elif relation['RelationType'] in female_indicators:
            person['Gender'] = 'Vrouw'
            # print('Set vrouw')

    # TODO Match on pid???
    # TODO make not case sensitive
    # TODO finish gender_identifier

# TODO finish set occurences
def set_occurences():
    return 0

def analyze_person(person, relation):
    gender_identifier(person, relation)
    # get_approx_age
    # other interesting stuff we can try to find


def analyze_people(people, relationEP):
    analyzed_people = []
    # Check how many people we have as input
    if isinstance(people,dict):
        # We have a single person as input
        analyze_person(people, relationEP)
        # get_approx_age
        analyzed_people = [people]
    else:
        # We have multiple people as input
        # Check if both the people list and the relationEP list have the same length
        if len(people)!=len(relationEP):
            print(len(people), len(relationEP))
            input("Press Enter to continue...")

        for person, relation in zip(people, relationEP):
            analyze_person(person, relation)
            analyzed_people.append(person)

    return analyzed_people

if __name__ == "__main__":
    client, bhic, source_collections, people = mongo_connect()

    print("-----Do you really want to overwrite the people collection?-----")
    input("Press Enter to continue...")
    people.remove({})
    print("All documents in people collection have been removed")
    input("Press Enter to continue...")

    # Loop over all collections containing information
    for collection in source_collections:
        # For all items in the current selection
        for n, record in enumerate(source_collections['marriage_actions'].find()):
            # print(item._id)
            # pp.pprint(item)
            # print('\n')
            print(record['_id'])
            # Check if there are people in this record
            if 'Person' in record and 'RelationEP' in record:
                analyzed_people = analyze_people(record['Person'], record['RelationEP'])

            print(analyzed_people)
            print("\n")


            for analyzed_person in analyzed_people:
                # TODO find out what the origin of the
                if '_id' in analyzed_person:
                    del analyzed_person['_id']
                people.insert_one(analyzed_person)
            # people.insert_many(analyzed_people)
            if n%20==0:
                print(collection)
                print('Current collection:', collection, 'Opetation:', n)
