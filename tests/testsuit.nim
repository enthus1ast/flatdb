## Testsuit for flatdb

import flatdb
import json
import os
import sequtils

# import 

block: # basic tests
  var db = newFlatDb("tests.db")
  assert db.nodes.len == 0
  var tst = %* {"foo": 1}
  var eid = db.append( tst )
  assert db[eid] == tst
  assert db.len == 1
  assert db.nodes.len == 1
  assert db.query( qs().lim(1) , equal("foo", 1) ) == @[tst]
  assert db.queryReverse( qs().lim(1) , equal("foo", 1) ) == @[tst]
  assert db.query( qs().lim(0) , equal("foo", 1) ) == @[]
  assert db.query( qs().skp(1) , equal("foo", 1) ) == @[]
  assert db.query( qs().skp(0).lim(0) , equal("foo", 1) ) == @[]
  assert db.query( qs().skp(0).lim(1) , equal("foo", 1) ) == @[tst]
  db.close()

# block:
  # TODO save some fun for later
  # var db = newFlatDb("tests.db")
  # var tst = %* {"foo": 1}
  # var tst2 = %* {"foo": 2}
  # var eid = db.append( tst )
  # var eid2 = db.append( tst2 )
  # db.len == 
  # db.close()

  # echo db[eid]
  # echo repr db.getNode(eid)





removeFile("tests.db")