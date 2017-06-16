flatdb
=======

a small flatfile, inprocess database for nim-lang

warning this is in development right now, expect some quirks (or even data losses)



usage
=====
```nim
import flatdb
import json

var db = newFlatDb("testdb.db", false)
discard db.load() # database has to be loaded before use
db.drop() # clear the testdb on every start


# Some test data (json)
let idFirst = db.append(%* {"foo":"baa", "hallo": "welt", "long": 80})
let idSecond = db.append(%* {"foo":"baa", "hallo": "david", "long": 123})
discard db.append(%* {"foo":"baz", "hallo": "nim", "long": 356})
discard db.append(%* {"a":"am", "a": "flat", "long": 567})


# use the id when you have it, the lookups are fast
echo db[idFirst].getOrDefault("foo").getStr()


# query the db, this for now touches every item in the db
# and find the match for you. (beginning from the top/the oldest item)
echo db.query equal("foo", "baa") 
echo db.queryOne equal("foo", "baz") and equal("hallo", "nim")
echo db.query(  (equal("foo", "baz") and equal("hallo", "nim")) or lower("long", 100) )


# you decide if it makes more sense for you to start the 
# query at the end of the table (at the oldest item); use the *Reverse procs.
echo db.queryReverse equal("foo", "baa") 
echo db.queryReverse(  (equal("foo", "baz") and equal("hallo", "nim")) or lower("long", 100) )


# if you use one of the *Reverse procs, the result set is reversed as well. 
# So to get the items in their natural order (flatdb preserves insertion order)
# one has to utilize the `reversed` proc from `algorithm` module.
import algorithm
echo (db.queryReverse equal("foo", "baa")).reverse()



# Delete by id
db.delete idFirst


# Delete by matcher
echo db.nodes
db.delete lower("long", 400) 
echo db.nodes
```
