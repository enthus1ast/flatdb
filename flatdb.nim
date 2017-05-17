import json
import streams
import tables
import hashes
import sequtils # atm only needed for tests
import os
import random
import strutils
import oids
randomize()


## this is the custom build 'database' for nimCh4t 
## this stores msg lines as json but seperated by "\n"
## This is not a real database, so expect a few quirks.

## This database is designed like:
##  - Mostly append only (append only is fast)
##     - Update is inefficent (has to write whole database again)



###########################################################################################
## Naive module for performance-testing
import times

# proc isFilled(hcode: Hash): bool {.inline.} =
#   result = hcode != 0

template timeIt*(name: string, p: untyped): stmt =
  ## Performs timing of the block call, and makes output into stdout.
  let timeStart = cpuTime()
  p
  echo name, ": ", formatFloat((cpuTime() - timeStart) * 1000000, precision=12), " μs"
###########################################################################################
type 
  FlatDb* = ref object 
    path*: string
    stream*: FileStream
    nodes*: OrderedTableRef[string, JsonNode]
    inmemory*: bool
    manualFlush*: bool # if this is set to true one has to manually call stream.flush() 
                       # else it gets called by every db.append()! 
                       # so set this to true if you want to append a lot of lines in bulk
                       # set this to false when finished and call db.stream.flush() once.
  
  Order* = enum
    ASC # search value string
    DESC

  EntryId* = string

  Matcher* = proc (x: JsonNode): bool 


proc newFlatDb*(path: string, inmemory: bool): FlatDb = 
  # if inmemory is set to true the filesystem gets not touched at all.

  result = FlatDb()
  result.path = path
  result.inmemory = inmemory
  if not inmemory:
    if not fileExists(path):
      open(path, fmWrite).close()    
    result.stream = newFileStream(path, fmReadWriteExisting)
    
  result.nodes = newOrderedTable[string, JsonNode](rightSize(200_000))
  result.manualFlush = false

proc genId*(): EntryId = 
  ## Mongo id compatible
  echo "generating id..."
  return $genOid()

proc append*(db: FlatDb, node: JsonNode, eid: EntryId = nil): EntryId = 
  ## appends a json node to the opened database file (line by line)
  ## even if the file already exists!
  ## if the 
  var id: EntryId
  
  if not eid.isNil:
    id = eid
  elif node.hasKey("_id"):
    id = node["_id"].getStr
  else:
    echo 84
    id = genId()
  
  if not db.inmemory:
    # var nodeAndId = node 
    if not node.hasKey("_id"):
      node["_id"] = %* id
    db.stream.writeLine($node)
  if not db.manualFlush:
    db.stream.flush()

  node.delete("_id") # we dont need the key in memory twice
  db.nodes.add(id, node) 
  return id

proc `[]`*(db: FlatDb, key: string): JsonNode = 
  return db.nodes[key]
  

proc backup*(db: FlatDb) =
  ## Creates a backup of the original db.
  ## We do this to avoid haveing the data only in memory.
  let backupPath = db.path & ".bak"
  removeFile(backupPath) # delete old backup
  copyFile(db.path, backupPath) # copy current db to backup path


proc drop*(db: FlatDb) = 
  ## DELETES EVERYTHING
  ## deletes the whole database.
  ## after this call we can use the database as normally
  db.stream.close()
  db.stream = newFileStream(db.path, fmWrite)
  db.nodes.clear()

proc store*(db: FlatDb, nodes: seq[JsonNode]) = 
  ## DELETES EVERYTHING
  ## write every json node to the db.
  ## overwriteing everything.
  echo "----------- Store got called on: ", db.path
  db.backup()
  db.drop()
  db.manualFlush = true
  # echo db.nodes
  for node in nodes:
    discard db.append(node)
  db.stream.flush()
  db.manualFlush = false

proc flush*(db: FlatDb) = 
  ## writes the whole memory database to file.
  ## overwrites everything.
  ## If you have done changes in db.nodes you have to call db.flush() to sync it to the filesystem
  # var allNodes = toSeq(db.nodes.values())
  echo "----------- Flush got called on: ", db.path
  var allNodes = newSeq[JsonNode]()
  for id, node in db.nodes.pairs():
    node["_id"] = %* id
    allNodes.add(node)
  db.store(allNodes)

proc load*(db: FlatDb): bool = 
  ## reads the complete flat db and returns true if load sucessfully, false otherwise
  var line: string  = ""
  var obj: JsonNode
  var id: EntryId
  var needForRewrite: bool = false
  db.nodes.clear()

  if db.stream.isNil():
    return false

  while db.stream.readLine(line):
    obj = parseJson(line)
    if not obj.hasKey("_id"):
        id = genId()
        needForRewrite = true
    else:
        id = obj["_id"].getStr()
        obj.delete("_id") # we already have the id as table key 
    db.nodes.add(id, obj)
  if needForRewrite:
    echo "Generated missing ids rewriting database"
    db.flush()
  return true

#### Reverse pair##########################################
# Copied from tables.nim
# template forAllOrderedPairs(yieldStmt: untyped): typed {.dirty.} =
#   var h = t.first
#   while h >= 0:
#     var nxt = t.data[h].next
#     if isFilled(t.data[h].hcode): yieldStmt
#     h = nxt

# iterator pairsReverse*[A, B](t: OrderedTable[A, B]): (A, B) =
#   ## iterates over any (key, value) pair in the table `t` in insertion
#   ## order.
#   forAllOrderedPairs:
#     yield (t.data[h].key, t.data[h].val)


# iterator pairsReverse*[A, B](t: Table[A, B]): (A, B) =
#   ## iterates over any (key, value) pair in the table `t`.
#   for h in high(t.data)..0:
#     if isFilled(t.data[h].hcode): yield (t.data[h].key, t.data[h].val)
##########################################################################
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

iterator queryIter*(db: FlatDb, matcher: proc (x: JsonNode): bool ): JsonNode = 
  for id, entry in db.nodes.pairs():
    if matcher(entry):
      entry["_id"] = % id
      yield entry

# iterator queryRevIter*(db: FlatDb, matcher: proc (x: JsonNode): bool ): JsonNode = 
#   # start search on the end of the nodes table
#   for id, entry in db.nodes.pairsReverse():
#     if matcher(entry):
#       entry["_id"] = % id
#       yield entry

iterator queryIter*(db: FlatDb, matchers: openarray[proc (x: JsonNode): bool] ): JsonNode = 
  var ok = true
  for id, entry in db.nodes.pairs():
    for matcher in matchers:
      ok = ok and matcher(entry)
    if ok:
      entry["_id"] = % id
      yield entry


proc query*(db: FlatDb, matcher: proc (x: JsonNode): bool ): seq[JsonNode] =
    return toSeq(db.queryIter(matcher))

# proc query*(db: FlatDb, matchers: openarray[proc (x: JsonNode): bool] ): seq[JsonNode] =
#     return toSeq(db.queryIter(matchers))

proc queryOne*(db: FlatDb, matcher: proc (x: JsonNode): bool ): JsonNode = 
  ## just like query but returns the first match only (iteration stops after first)
  for entry in db.queryIter(matcher):
    if matcher(entry):
      return entry
  return nil

proc queryOne*(db: FlatDb, id: EntryId, matcher: proc (x: JsonNode): bool ): JsonNode = 
  ## returns the entry with `id` and also matching on matcher, if you have the _id, use it, its fast.
  if not db.nodes.hasKey(id):
    return nil
  if matcher(db.nodes[id]):
    return db.nodes[id]
  return nil

proc exists*(db: FlatDb, matcher: proc (x: JsonNode): bool ): bool =
  ## returns true if we found at least one match
  if db.queryOne(matcher).isNil:
    return false
  return true

proc notExists*(db: FlatDb, matcher: proc (x: JsonNode): bool ): bool =
  ## returns false if we found no match
  if db.queryOne(matcher).isNil:
    return true
  return false

# template matcher(stmt):
  # proc (x: JsonNode): bool = 
      # yield
      # stmt
      # return x[key].getStr() == val



proc equal*(key: string, val: string): proc = 
  return proc (x: JsonNode): bool = 
    return x.getOrDefault(key).getStr() == val
proc equal*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool = 
    return x.getOrDefault(key).getNum() == val
proc equal*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool = 
    return x.getOrDefault(key).getFnum() == val
proc equal*(key: string, val: bool): proc = 
  return proc (x: JsonNode): bool = 
    return x.getOrDefault(key).getBool() == val


proc lower*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getNum < val
proc lower*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFnum < val
proc lowerEqual*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getNum <= val
proc lowerEqual*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFnum <= val


proc higher*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getNum > val
proc higher*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFnum > val
proc higherEqual*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getNum >= val
proc higherEqual*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFnum >= val

proc between*(key: string, fromVal:float, toVal: float): proc =
  return proc (x: JsonNode): bool = 
    let val = x.getOrDefault(key).getFnum
    val > fromVal and val < toVal
proc between*(key: string, fromVal:int, toVal: int): proc =
  return proc (x: JsonNode): bool = 
    let val = x.getOrDefault(key).getNum
    val > fromVal and val < toVal
proc betweenEqual*(key: string, fromVal:float, toVal: float): proc =
  return proc (x: JsonNode): bool = 
    let val = x.getOrDefault(key).getFnum
    val >= fromVal and val <= toVal
proc betweenEqual*(key: string, fromVal:int, toVal: int): proc =
  return proc (x: JsonNode): bool = 
    let val = x.getOrDefault(key).getNum
    val >= fromVal and val <= toVal

proc has*(key: string): proc = 
  return proc (x: JsonNode): bool = return x.hasKey(key)

proc `and`*(p1, p2: proc (x: JsonNode): bool): proc (x: JsonNode): bool =
  return proc (x: JsonNode): bool = return p1(x) and p2(x)

proc `or`*(p1, p2: proc (x: JsonNode): bool): proc (x: JsonNode): bool =
  return proc (x: JsonNode): bool = return p1(x) or p2(x)

proc `not`*(p1: proc (x: JsonNode): bool): proc (x: JsonNode): bool =
  return proc (x: JsonNode): bool = return not p1(x)



# proc sort(nodes: seq[JsonNode], keyname:string, order: Order ): seq[JsonNode] = 
#   discard

proc close(db: FlatDb) = 
  db.stream.flush()
  db.stream.close()

proc keepIf*(db: FlatDb, matcher: proc) = 
  ## filters the database file, only lines that match `matcher`
  ## will be in the new file.
  ##
  ## This does the filtering in a special way,  
  ## it creates a tmp database, writes entry by entry in this file
  ## then moves tmp database to original db. 
  ##
  ## this creates a tmp file!
  let tmpPath = db.path & ".tmp"
  var tmpDb = newFlatDb(tmpPath, false)
  tmpDb.store db.query(matcher)
  tmpDb.close()
  db.stream.close()
  removeFile db.path
  moveFile(tmpPath, db.path)
  db.stream = newFileStream(db.path, fmReadWriteExisting)  
  discard db.load()


proc delete*(db: FlatDb, id: EntryId) =
  ## deletes entry by id, respects `manualFlush`
  var hit = false
  if db.nodes.hasKey(id):
      hit = true
      db.nodes.del(id)
  if not db.manualFlush and hit:
    db.flush()


proc delete*(db: FlatDb, matcher: proc (x: JsonNode): bool ) =
  ## deletes entry by matcher, respects `manualFlush`
  ## TODO make this in the new form (withouth truncate every time)  
  var hit = false
  for item in db.queryIter( matcher ):
    hit = true
    db.nodes.del(item["_id"].getStr)
  if not db.manualFlush and hit:
    db.flush()


when isMainModule:
  ## tests
  # block:
    # var db = newFlatDb("test.db", false)
    # db.drop()
    # var orgEntry = %* {"my": "test"}
    # var id = db.append(orgEntry)
    # assert db.nodes[id] == orgEntry
    # assert toSeq(db.nodes.values())[0] == orgEntry
    # db.drop()
    # assert db.nodes.hasKey(id) == false
    # db.close()

  block:
    # fast write test
    let howMany = 10

    var db = newFlatDb("test.db", false)
    db.drop()
    var ids = newSeq[EntryId]()
    for each in 0..howMany:
      # echo "w:", each
      var entry = %* {"foo": each}
      ids.add db.append(entry)
    # echo db.nodes
    db.close()

    # quit()
    # Now read everything again and check if its good
    db = newFlatDb("test.db", false)
    assert true == db.load()
    var cnt = 0
    for id in ids:
      var entry = %* {"foo": cnt}
      # echo entry
      assert db.nodes[id] == entry
      cnt.inc

    # echo "passed"
    # Test if table preserves order
    var idx = 0
    for each in db.nodes.values():
      var entry = %* {"foo": idx}
      assert each == entry
      idx.inc
    db.close()

  block:
    var db = newFlatDb("test.db", false)
    # assert true == db.load()
    db.drop()

    for each in 0..100:
      var entry = %* {"foo": each}
      discard db.append(entry)
    db.close()

    # db.truncate(matcher: x["foo"].getNum mod 2 == 0 )
    db.keepIf(proc(x: JsonNode): bool = return x["foo"].getNum mod 2 == 0 )
    echo toSeq(db.nodes.values())[0]
    db.keepIf(proc(x: JsonNode): bool = return x["foo"].getNum mod 2 == 0 )
    echo toSeq(db.nodes.values())[0]
    # quit()

  block: #bug outofbound
    var db = newFlatDb("test.db", false)
    discard db.load()
    var entry = %* {"type":"message","to":"lobby","from":"sn0re","content":"asd"}
    discard db.append(entry)


  block: 
      # an example of an "update"
      var db = newFlatDb("test.db", false)
      db.drop()

      # testdata
      var entry: JsonNode
      entry = %* {"user":"sn0re", "password": "asdflkjsaflkjsaf"}
      discard db.append(entry)  

      entry = %* {"user":"klaus", "password": "hahahahsdfksafjj"}
      let id =  db.append(entry)  

      # The actual update
      db.nodes[id]["password"] = % "123"
      # echo db.nodes

      #### Now a matcher example
      # timeIt "haha":
      #   echo db.query equal("user","sn0re") 
      #   echo db.query equal("user","sn0re") 
      #   echo db.query equal("user","sn0re") 
      #   echo db.query equal("user","sn0re") 
      #   # echo db.query equal("user","sn0re") 

      db.flush()



  block:
      var db = newFlatDb("test.db", false)
      db.drop()

      # testdata
      var entry: JsonNode
      entry = %* {"user":"sn0re", "password": "pw1"}
      discard db.append(entry)      

      entry = %* {"user":"sn0re", "password": "pw2"}
      discard db.append(entry)      

      entry = %* {"user":"sn0re", "password": "pw3"}
      discard db.append(entry)      

      entry = %* {"user":"klaus", "password": "asdflkjsaflkjsaf"}
      discard db.append(entry)

      entry = %* {"user":"uggu", "password": "asdflkjsaflkjsaf"}
      discard db.append(entry)

      assert db.query((equal("user", "sn0re") and equal("password", "pw2")))[0]["password"].getStr == "pw2"
      let res = db.query((equal("user", "sn0re") and (equal("password", "pw2") or equal("password", "pw1"))))
      assert res[0]["password"].getStr == "pw1"
      assert res[1]["password"].getStr == "pw2"
      # quit()

  block:
      var db = newFlatDb("test.db", false)
      db.drop()

      # testdata
      var entry: JsonNode

      entry = %* {"user":"sn0re", "timestamp": 10.0}
      discard db.append(entry)  

      entry = %* {"user":"sn0re", "timestamp": 100.0}
      discard db.append(entry)  

      entry = %* {"user":"klaus", "timestamp": 200.0}
      discard db.append(entry)  

      entry = %* {"user":"klaus", "timestamp": 250.0}
      discard db.append(entry)  

      echo db.query higher("timestamp", 150.0)
      echo db.query higher("timestamp", 150.0) and lower("timestamp", 210.0)
      echo db.query equal("user", "sn0re") and  (higher("timestamp", 20.0) and ( lower("timestamp", 210.0) ) )

      var res = db.query(not(equal("user", "sn0re")))
      echo res
      echo res[0] == db[res[0]["_id"].getStr]





when isMainModule and defined doNotRun:
  var db = newFlatDb("test.db", false)
  assert db.load() == true
  var entry = %* {"some": "json", "things": [1,2,3]}
  var entryId = db.append(entry)
  entryId = db.append(entry)
  echo entryId
  echo db.nodes[entryId]
  # for each in db.nodes.values():
    # echo each

  entry = %* {"some": "json", "things": [1,3]}
  entryId = db.append(entry)

  entry = %* {"hahahahahahhaha": "json", "things": [1,2,3]}
  entryId = db.append(entry)

  proc m1(m:JsonNode): bool = 
    # a custom query example
    # test if things[1] == 2
    if not m.hasKey("things"):
      return false
    if not m["things"].len > 1:
      return false
    if m["things"][1].getNum == 2:
      return true
    return false

  echo db.query(m1)
  # entryId = db.append(entry)
  # entryId = db.append(entry)
  # entryId = db.append(entry)




