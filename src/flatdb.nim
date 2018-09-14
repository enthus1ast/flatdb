#
#
#                     flatdb
#        (c) Copyright 2017 David Krause
#
#    See the file "licence", included in this
#    distribution, for details about the copyright.

import json, streams, hashes, sequtils, os, random, strutils, oids
import flatdbtable
export flatdbtable
randomize() # TODO call this at Main?

## this is the custom build 'database' for nimCh4t 
## this stores msg lines as json but seperated by "\n"
## This is not a _real_ database, so expect a few quirks.

## This database is designed like:
##  - Mostly append only (append only is fast)
##  - Update is inefficent (has to write whole database again)
type 
  Limit = int
  FlatDb* = ref object 
    path*: string
    stream*: FileStream
    nodes*: FlatDbTable
    inmemory*: bool
    manualFlush*: bool ## if this is set to true one has to manually call stream.flush() 
                       ## else it gets called by every db.append()! 
                       ## so set this to true if you want to append a lot of lines in bulk
                       ## set this to false when finished and call db.stream.flush() once.
                       ## TODO should this be db.stream.flush or db.flush??
  EntryId* = string
  Matcher* = proc (x: JsonNode): bool 
  QuerySettings* = ref object
    limit*: int # end query if the result set has this amounth of entries
    skip*: int # skip the first n entries 

# Query Settings ---------------------------------------------------------------------
proc lim*(settings = QuerySettings(), cnt: int): QuerySettings =
  ## limit the smounth of matches
  result = settings
  result.limit = cnt

proc skp*(settings = QuerySettings(), cnt: int): QuerySettings =
  ## skips amounth of matches
  result = settings
  result.skip = cnt

proc newQuerySettings*(): QuerySettings =
  ## Configure the queries, skip, limit found elements ..
  result = QuerySettings()
  result.skip = -1
  result.limit = -1

proc qs*(): QuerySettings =
  ## Shortcut for newQuerySettings
  result = newQuerySettings()
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

proc newFlatDb*(path: string, inmemory: bool = false): FlatDb = 
  # if inmemory is set to true the filesystem gets not touched at all.
  result = FlatDb()
  result.path = path
  result.inmemory = inmemory
  if not inmemory:
    if not fileExists(path):
      open(path, fmWrite).close()    
    result.stream = newFileStream(path, fmReadWriteExisting)
  result.nodes = newFlatDbTable()
  result.manualFlush = false

proc genId*(): EntryId = 
  ## Mongo id compatible
  return $genOid()

proc append*(db: FlatDb, node: JsonNode, eid: EntryId = ""): EntryId = 
  ## appends a json node to the opened database file (line by line)
  ## even if the file already exists!
  var id: EntryId
  if not (eid == ""):
    id = eid
  elif node.hasKey("_id"):
    id = node["_id"].getStr
  else:
    id = genId()
  if not db.inmemory:
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
  ## write every json node to the db.
  ## overwriteing everything.
  ## but creates a backup first
  echo "----------- Store got called on: ", db.path
  db.backup()
  db.drop()
  db.manualFlush = true
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
        obj.delete("_id") # we already have the id as table key 
    db.nodes.add(id, obj)
  if needForRewrite:
    echo "Generated missing ids rewriting database"
    db.flush()
  return true

proc len*(db: FlatDb): int = 
  return db.nodes.len()

proc getNode*(db: FlatDb, key: EntryId): Node =
  return db.nodes.getNode($key)

# ----------------------------- Query Iterators -----------------------------------------
template queryIterImpl(direction: untyped, settings: QuerySettings, matcher: Matcher) = 
  var founds: int = 0
  var skipped: int = 0
  for id, entry in direction():
    if matcher(entry):
      if settings.skip != -1 and skipped < settings.skip:
        skipped.inc
        continue
      if founds == settings.limit and settings.limit != -1:
        break
      else:
        founds.inc
      entry["_id"] = % id
      yield entry

iterator queryIter*(db: FlatDb, matcher: Matcher ): JsonNode = 
  let settings = newQuerySettings()
  queryIterImpl(db.nodes.pairs, settings, matcher)

iterator queryIterReverse*(db: FlatDb, matcher: Matcher ): JsonNode = 
  let settings = newQuerySettings()
  queryIterImpl(db.nodes.pairsReverse, settings, matcher)

iterator queryIter*(db: FlatDb, settings: QuerySettings,  matcher: Matcher ): JsonNode = 
  queryIterImpl(db.nodes.pairs, settings, matcher)

iterator queryIterReverse*(db: FlatDb, settings: QuerySettings, matcher: Matcher ): JsonNode = 
  queryIterImpl(db.nodes.pairsReverse, settings, matcher)

# ----------------------------- Query -----------------------------------------
template queryImpl*(direction: untyped, settings: QuerySettings, matcher: Matcher)  = 
  return toSeq(direction(settings, matcher))

proc query*(db: FlatDb, matcher: Matcher ): seq[JsonNode] =
  let settings = newQuerySettings()
  queryImpl(db.queryIter, settings, matcher)

proc query*(db: FlatDb, settings: QuerySettings, matcher: Matcher ): seq[JsonNode] =
  queryImpl(db.queryIter, settings, matcher)

proc queryReverse*(db: FlatDb, matcher: Matcher ): seq[JsonNode] =
  let settings = newQuerySettings()
  queryImpl(db.queryIterReverse, settings, matcher)

proc queryReverse*(db: FlatDb, settings: QuerySettings,  matcher: Matcher ): seq[JsonNode] =
  queryImpl(db.queryIterReverse, settings, matcher)

# ----------------------------- QueryOne -----------------------------------------
template queryOneImpl(direction: untyped, matcher: Matcher) = 
  for entry in direction(matcher):
    if matcher(entry):
      return entry
  return nil  

proc queryOne*(db: FlatDb, matcher: Matcher ): JsonNode = 
  ## just like query but returns the first match only (iteration stops after first)
  queryOneImpl(db.queryIter, matcher)
proc queryOneReverse*(db: FlatDb, matcher: Matcher ): JsonNode = 
  ## just like query but returns the first match only (iteration stops after first)
  queryOneImpl(db.queryIterReverse, matcher)

proc queryOne*(db: FlatDb, id: EntryId, matcher: Matcher ): JsonNode = 
  ## returns the entry with `id` and also matching on matcher, if you have the _id, use it, its fast.
  if not db.nodes.hasKey(id):
    return nil
  if matcher(db.nodes[id]):
    return db.nodes[id]
  return nil

proc exists*(db: FlatDb, matcher: Matcher ): bool =
  ## returns true if we found at least one match
  if queryOne(db, matcher).isNil:
    return false
  return true

proc notExists*(db: FlatDb, matcher: Matcher ): bool =
  ## returns false if we found no match
  if db.queryOne(matcher).isNil:
    return true
  return false

# ----------------------------- Matcher -----------------------------------------
proc equal*(key: string, val: string): proc = 
  return proc (x: JsonNode): bool {.closure.} = 
    return x.getOrDefault(key).getStr() == val
proc equal*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool {.closure.} = 
    return x.getOrDefault(key).getInt() == val
proc equal*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool {.closure.} = 
    return x.getOrDefault(key).getFloat() == val
proc equal*(key: string, val: bool): proc = 
  return proc (x: JsonNode): bool {.closure.} = 
    return x.getOrDefault(key).getBool() == val

proc lower*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getInt < val
proc lower*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFloat < val
proc lowerEqual*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getInt <= val
proc lowerEqual*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFloat <= val

proc higher*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getInt > val
proc higher*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFloat > val
proc higherEqual*(key: string, val: int): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getInt >= val
proc higherEqual*(key: string, val: float): proc = 
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFloat >= val

proc dbcontains*(key: string, val: string): proc = 
  return proc (x: JsonNode): bool = 
    let str = x.getOrDefault(key).getStr()
    return str.contains(val)
proc dbcontainsInsensitive*(key: string, val: string): proc = 
  return proc (x: JsonNode): bool = 
    let str = x.getOrDefault(key).getStr()
    return str.toLowerAscii().contains(val.toLowerAscii())

proc between*(key: string, fromVal:float, toVal: float): proc =
  return proc (x: JsonNode): bool = 
    let val = x.getOrDefault(key).getFloat
    val > fromVal and val < toVal
proc between*(key: string, fromVal:int, toVal: int): proc =
  return proc (x: JsonNode): bool = 
    let val = x.getOrDefault(key).getInt
    val > fromVal and val < toVal
proc betweenEqual*(key: string, fromVal:float, toVal: float): proc =
  return proc (x: JsonNode): bool = 
    let val = x.getOrDefault(key).getFloat
    val >= fromVal and val <= toVal
proc betweenEqual*(key: string, fromVal:int, toVal: int): proc =
  return proc (x: JsonNode): bool = 
    let val = x.getOrDefault(key).getInt
    val >= fromVal and val <= toVal

proc has*(key: string): proc = 
  return proc (x: JsonNode): bool = return x.hasKey(key)

proc `and`*(p1, p2: proc (x: JsonNode): bool): proc (x: JsonNode): bool =
  return proc (x: JsonNode): bool = return p1(x) and p2(x)

proc `or`*(p1, p2: proc (x: JsonNode): bool): proc (x: JsonNode): bool =
  return proc (x: JsonNode): bool = return p1(x) or p2(x)

proc `not`*(p1: proc (x: JsonNode): bool): proc (x: JsonNode): bool =
  return proc (x: JsonNode): bool = return not p1(x)

proc close*(db: FlatDb) = 
  db.stream.flush()
  db.stream.close()

proc keepIf*(db: FlatDb, matcher: proc) = 
  ## filters the database file, only lines that match `matcher`
  ## will be in the new file.
  # TODO 
  db.store db.query matcher

proc delete*(db: FlatDb, id: EntryId) =
  ## deletes entry by id, respects `manualFlush`
  var hit = false
  if db.nodes.hasKey(id):
      hit = true
      db.nodes.del(id)
  if not db.manualFlush and hit:
    db.flush()

template deleteImpl(db: FlatDb, direction: untyped, matcher: Matcher) = 
  var hit = false
  for item in direction(matcher):
    hit = true
    db.nodes.del(item["_id"].getStr)
  if (not db.manualFlush) and hit:
    db.flush()
template delete*(db: FlatDb, matcher: Matcher) =
  ## deletes entry by matcher, respects `manualFlush`
  deleteImpl(db, db.queryIter, matcher)
template deleteReverse*(db: FlatDb, matcher: Matcher ) =
  ## deletes entry by matcher, respects `manualFlush`
  deleteImpl(db, db.queryIterReverse, matcher)

when isMainModule:
  import algorithm
  block:
    # fast write test
    let howMany = 10
    var db = newFlatDb("test.db", false)
    db.drop()
    var ids = newSeq[EntryId]()
    for each in 0..howMany:
      var entry = %* {"foo": each}
      ids.add db.append(entry)
    db.close()

    # Now read everything again and check if its good
    db = newFlatDb("test.db", false)
    assert true == db.load()
    var cnt = 0
    for id in ids:
      var entry = %* {"foo": cnt}
      assert db.nodes[id] == entry
      cnt.inc

    # Test if table preserves order
    var idx = 0
    for each in db.nodes.values():
      var entry = %* {"foo": idx}
      assert each == entry
      idx.inc
    db.close()

  block:
    var db = newFlatDb("test.db", false)
    db.drop()

    for each in 0..100:
      var entry = %* {"foo": each}
      discard db.append(entry)
    db.close()

    db.keepIf(proc(x: JsonNode): bool = return x["foo"].getInt mod 2 == 0 )
    echo toSeq(db.nodes.values())[0]
    db.keepIf(proc(x: JsonNode): bool = return x["foo"].getInt mod 2 == 0 )
    echo toSeq(db.nodes.values())[0]
    db.close()

  block: #bug outofbound
    var db = newFlatDb("test.db", false)
    discard db.load()
    var entry = %* {"type":"message","to":"lobby","from":"sn0re","content":"asd"}
    discard db.append(entry)
    db.close()

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
      db.flush()
      db.close()

  block :
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
      db.close()
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
      db.close()

  block:
      var db = newFlatDb("test.db", false)
      db.drop()

      # testdata
      var entry: JsonNode

      entry = %* {"user":"sn0re", "id": 1}
      discard db.append(entry)  

      entry = %* {"user":"sn0re", "id": 2}
      discard db.append(entry)  

      entry = %* {"user":"sn0re", "id": 3}
      discard db.append(entry)  

      entry = %* {"user":"sn0re", "id": 4}
      discard db.append(entry)    

      echo "######"
      echo db.queryReverse( qs().lim(2), equal("user", "sn0re") ).reversed()
      echo "^^^^^^"
      db.close()

when isMainModule and defined doNotRun:
  var db = newFlatDb("test.db", false)
  assert db.load() == true
  var entry = %* {"some": "json", "things": [1,2,3]}
  var entryId = db.append(entry)
  entryId = db.append(entry)
  echo entryId
  echo db.nodes[entryId]

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
    if m["things"][1].getInt == 2:
      return true
    return false

  echo db.query(m1)
  db.close()

when isMainModule:
  # clear up directory
  removeFile("test.db")
  removeFile("test.db.bak")