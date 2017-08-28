import flatdb
import json
import tables

var db = newFlatDb("tst.db")
# echo db.load()
# echo db.nodes
discard db.append( %* {"foo": "baa", "ug": 123, "_id": "1"} )
discard db.append( %* {"foo": "123", "ug": 333, "_id": "2"} )
discard db.append( %* {"foo": "444", "ug": 333, "_id": "3"} )
discard db.append( %* {"foo": "555", "ug": 333, "_id": "4"} )
discard db.append( %* {"foo": "666", "ug": 333, "_id": "5"} )

echo db.query equal("foo", "123")
echo db.query higher("ug", 200) and equal("foo", "nope")

for each in db.nodes.values:
  echo "val: ", each

for each in db.nodes.pairs:
  echo "pair: ", each  



proc getFlatDb*(): Flatdb {.exportc.} =
  return db