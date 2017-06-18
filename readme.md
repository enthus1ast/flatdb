flatdb
=======

a small flatfile, inprocess database for nim-lang

warning this is in development right now, expect some quirks (or even data losses)


what
=====

- holds your data in memory with a table + doubly linked list, to provide 
	- fast hash acces with keys
	- insertion order ( iterate in both directions )

- persists the data to a `jsonl` (json line by line) file.


Be aware that if you change an entry in memory, you have to call "db.flush" manually!


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

# Kinda "transaction"
# if you know there is alot to write, disable the autoflush temporarily.
# Flatdb will then not reflect changes to the filesystem until db.flush() is called manually.
db.autoflush = false
for each in 0..100:
  db.append({"foo": % each})
db.autoflush = true
db.flush()


# Query settings
# use `newQuerySettings` or the `qs()` alias to create "QuerySettings", where you could
# - limit the the result set with `lmt(n)` or   
# - skip over matches with `skp(n)`
# this example skips over the first 5 matches and limits the result set to 10 items.
echo db.query(qs().skp(5).lmt(10) , higher("someKey", 10) and equal("room", "lobby") )


# Queries the db from oldest to newest so:
# this eample skips over the first 5 matches _from behind_ and limits the result set to 10 items.
echo db.queryReverse(qs().skp(5).lmt(10) , higher("someKey", 10) and equal("room", "lobby") )

# Update a bunch of entries example
for entry in db.query( qs().lmt(10).skp(5), equal("room", "lobby") and equal("topic", "") ):
  entry["topic"] = % "DEBUG TOPIC!"
db.flush()
```

