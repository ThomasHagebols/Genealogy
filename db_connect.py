# -*- coding: utf-8 -*-
from config import *
from pymongo import MongoClient
from bson.objectid import ObjectId
import pprint

pp = pprint.PrettyPrinter(indent=2)

# Connect to the database
def mongo_connect():
    client = MongoClient('mongodb://' + username() + ":" + password() + "@"  + ip() + ":" + str(port()) + '/')
    bhic = client['bhic-databases']

    # Load collections
    births = bhic['civil-status-births-certificates']
    baptisms = bhic['dtb-baptisms-certificates']
    marriage_acts = bhic['civil-status-marriage-acts']
    marriage_actions = bhic['dtb-marriage-actions']
    deaths = bhic['civil-status-deaths']
    death_actions = bhic['dtb-death-actions']
    succession = bhic['memories-of-succession']
    pop_registers = bhic['genealogical-population-registers']
    military = bhic['military-register']
    prison = bhic['prision-register']
    people = bhic['people']
    errors = bhic['people']

    # Dict containing all collections of the original data source (as imported)
    source_collections = {'births':births, 'baptisms':baptisms,
    'marriage_acts':marriage_acts, 'marriage_actions':marriage_actions,
    'deaths':deaths, 'death_actions':death_actions, 'succession':succession,
    'pop_registers':pop_registers, 'military':military, 'prison':prison}

    return client, bhic, source_collections, people, errors

# Query all collections in the "source_collections" dictionary
def query_all_source_collections(query, nr_results, verbose):
    client, bhic, source_collections, people = mongo_connect()

    for collection in source_collections:
        if verbose == True:
            for n, document in enumerate(source_collections[collection].find(query)):
                pp.pprint(document)

                if n>nr_results:
                    break

        print("# records in", collection, source_collections[collection].find(query).count())


if __name__ == "__main__":
    client, bhic, source_collections, people, errors = mongo_connect()

    # test by counting # records in each collection
    for collection in source_collections:
        # print(type(collection))
        print("# records in", collection, source_collections[collection].count())

    print("# records in people", people.count())
