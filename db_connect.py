# -*- coding: utf-8 -*-
from config import *
from pymongo import MongoClient
from bson.objectid import ObjectId
import pprint

pp = pprint.PrettyPrinter(indent=2)

# Connect to the database
def mongo_connect():
    client = MongoClient('mongodb://' + username() + ":" + password() + "@"  + ip() + ":" + str(port()) + '/')
    bhic = client['local']

    # Load collections
    succession = bhic['1-memories-of-succession']
    baptisms = bhic['10-dtb-baptisms-certificates']
    pop_registers = bhic['2-genealogical-population-registers']
    births = bhic['3-civil-status-births-certificates']
    marriage_acts = bhic['4-civil-status-marriage-acts']
    deaths = bhic['5-civil-status-deaths']
    military = bhic['6-military-register']
    prison = bhic['7-prison-register']
    marriage_actions = bhic['8-dtb-marriage-actions']
    death_actions = bhic['9-dtb-death-actions']
    people = bhic['people']
    people_debug = bhic['people_debug']
    errors = bhic['errors']

    # Dict containing all collections of the original data source (as imported)
    source_collections = {'3-civil-status-births-certificates':births, '10-dtb-baptisms-certificates':baptisms,
                          '4-civil-status-marriage-acts':marriage_acts, '8-dtb-marriage-actions':marriage_actions,
                          '5-civil-status-deaths':deaths, '9-dtb-death-actions':death_actions,
                          '1-memories-of-succession':succession,'2-genealogical-population-registers':pop_registers,
                          '6-military-register':military, '7-prison-register':prison}

    return {'client': client, 'bhic': bhic,
            'source_collections': source_collections,
            'people':people, 'people_debug': people_debug, 'errors': errors}

# Query all collections in the "source_collections" dictionary
def query_all_source_collections(query, nr_results, verbose):
    mc = mongo_connect()

    for collection in mc['source_collections']:
        if verbose == True:
            for n, document in enumerate(mc['source_collections'][collection].find(query)):
                pp.pprint(document)

                if n>nr_results:
                    break

        print("# records in", collection, mc['source_collections'][collection].find(query).count())


if __name__ == "__main__":
    mc = mongo_connect()

    # test by counting # records in each collection
    for collection in mc['source_collections']:
        # print(type(collection))
        print("# records in", collection, mc['source_collections'][collection].count())

    print("# records in people", mc['people'].count())
