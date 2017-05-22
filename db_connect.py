# -*- coding: utf-8 -*-
from config import *
from pymongo import MongoClient

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
    pop_resisters = bhic['genealogical-population-registers']
    military = bhic['military-register']
    prison = bhic['prision-register']
    people = bhic['people']

    # Dict containing all collections of the original data source (as imported)
    source_collections = {'births':births, 'baptisms':baptisms,
    'marriage_acts':marriage_acts, 'marriage_actions':marriage_actions,
    'deaths':deaths, 'death_actions':death_actions, 'succession':succession,
    'pop_resisters':pop_resisters, 'military':military, 'prison':prison}

    return client, bhic, source_collections, people

if __name__ == "__main__":
    client, bhic, source_collections, people = mongo_connect()

    # test by counting # records in each collection
    for collection in source_collections:
        # print(type(collection))
        print("# records in", collection, source_collections[collection].count())

    print("# records in people", people.count())
