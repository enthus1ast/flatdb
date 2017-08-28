import flatdb
import json
import tables

var db = newFlatDb("tst.db")
echo db.load()
# echo db.nodes
discard db.append( %* {"foo": "baa", "ug": 123} )
discard db.append( %* {"foo": "123", "ug": 333} )

echo db.query equal("foo", "123")
echo db.query higher("ug", 200) and equal("foo", "nope")

for each in db.nodes.values:
  echo each

for each in db.nodes.pairs:
  echo each  



proc getFlatDb*(): Flatdb {.exportc.} =
  return db