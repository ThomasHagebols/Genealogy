# Genealogy
A family tree of the Noord-Brabant region

The passwords of the connectors are stored in the config.py file. This file is included on the gitignore so you should add it yourself by copying the snippet and inserting the correct username, password, ip address and port.

```python
def username():
    username = "username"
    return username

def password():
    password = "password"
    return password

def ip():
    address = "ip address"
    return address

def port():
    port = int[port]
    return port    
```
To test the connection run the db_connect.py as a standalone script. This should give the following output (though the number of records might be a little different)

```
# records in baptisms 973551
# records in births 723950
# records in deaths 1145504
# records in pop_resisters 1682984
# records in marriage_acts 456851
# records in prison 104427
# records in marriage_actions 302116
# records in military 110036
# records in succession 440339
# records in death_actions 344140
# records in people 2824
```

You can connect to the mongodb using db_connect.py with the following snippit

```python
from db_connect import mongo_connect

mc = mongo_connect()
```

example of a query which counts all documents in a collection

```python
for collection in mc['source_collections']:
    print("# records in", collection, mc['source_collections'][collection].find({}).count())

```
