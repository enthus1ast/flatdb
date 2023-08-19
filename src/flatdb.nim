#
#
#                     flatdb
#        (c) Copyright 2023 David Krause
#
#    See the file "licence", included in this
#    distribution, for details about the copyright.

import json, streams, hashes, sequtils, os, random, strutils, oids
import flatdb/flatdbtable
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

template genId*(): EntryId =
  ## Mongo id compatible
  $genOid()

proc append*(db: FlatDb, node: JsonNode, eid: EntryId = "", doFlush = true): EntryId {.discardable.} =
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
  if doFlush:
    db.stream.flush()
  node.delete("_id") # we dont need the key in memory twice
  db.nodes.add(id, node)
  return id
  
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
  # when not defined(release):
  #   echo "----------- Store got called on: ", db.path
  db.backup()
  db.drop()
  for node in nodes:
    discard db.append(node, doFlush = false)
  db.stream.flush()


proc flush*(db: FlatDb) =
  ## writes the whole memory database to file.
  ## overwrites everything.
  ## If you have done changes in db.nodes you have to call db.flush() to sync it to the filesystem
  # var allNodes = toSeq(db.nodes.values())
  # when not defined(release):
  #   echo "----------- Flush got called on: ", db.path
  var allNodes = newSeq[JsonNode]()
  for id, node in db.nodes.pairs():
    node["_id"] = %* id
    allNodes.add(node)
  db.store(allNodes)

proc load*(db: FlatDb): bool {.discardable.} = 
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

proc update*(db: FlatDb, key: string, value: JsonNode, doFlush = true) =
  ## Updates an entry, if `flush == true` database gets flushed afterwards
  ## Updateing the db is expensive!
  db.nodes[key] = value
  if doFlush:
    db.flush()

template `[]`*(db: FlatDb, key: string): JsonNode =
  db.nodes[key]

template `[]=`*(db: FlatDb, key: string, value: JsonNode, doFlush = true) =
  ## see `update`
  db.update(key, value, doFlush)

template len*(db: FlatDb): int =
  db.nodes.len()

template getNode*(db: FlatDb, key: EntryId): Node =
  db.nodes.getNode($key)

# ----------------------------- Query Iterators -----------------------------------------
template queryIterImpl(direction: untyped, settings: QuerySettings, matcher: Matcher = nil) =
  var founds: int = 0
  var skipped: int = 0
  for id, entry in direction():
    if matcher.isNil or matcher(entry):
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
template queryImpl*(direction: untyped, settings: QuerySettings, matcher: Matcher) =
  return toSeq(direction(settings, matcher))

proc query*(db: FlatDb, matcher: Matcher ): seq[JsonNode] =
  let settings = newQuerySettings()
  queryImpl(db.queryIter, settings, matcher)

proc query*(db: FlatDb, settings: QuerySettings, matcher: Matcher ): seq[JsonNode] =
  queryImpl(db.queryIter, settings, matcher)

proc queryReverse*(db: FlatDb, matcher: Matcher ): seq[JsonNode] =
  let settings = newQuerySettings()
  queryImpl(db.queryIterReverse, settings, matcher)

proc queryReverse*(db: FlatDb, settings: QuerySettings, matcher: Matcher ): seq[JsonNode] =
  queryImpl(db.queryIterReverse, settings, matcher)

# ----------------------------- all ----------------------------------------------
iterator items*(db: FlatDb, settings = qs()): JsonNode =
  queryIterImpl(db.nodes.pairs, settings)

iterator itemsReverse*(db: FlatDb, settings = qs()): JsonNode =
  queryIterImpl(db.nodes.pairsReverse, settings)

# TODO ?
# db.nodes.pairs
# iterator pairs*(db: FlatDb): tuple(EntryId, JsonNode) =
#   ## yields all key, values (unfiltered) from db

# ----------------------------- QueryOne -----------------------------------------
template queryOneImpl(direction: untyped, matcher: Matcher) =
  for entry in direction(matcher):
    if matcher(entry):
      return entry
  return nil

proc queryOne*(db: FlatDb, matcher: Matcher): JsonNode =
  ## just like query but returns the first match only (iteration stops after first)
  queryOneImpl(db.queryIter, matcher)
proc queryOneReverse*(db: FlatDb, matcher: Matcher): JsonNode =
  ## just like query but returns the first match only (iteration stops after first)
  queryOneImpl(db.queryIterReverse, matcher)

proc queryOne*(db: FlatDb, id: EntryId, matcher: Matcher): JsonNode =
  ## returns the entry with `id` and also matching on matcher, if you have the _id, use it, its fast.
  if not db.nodes.hasKey(id):
    return nil
  if matcher(db.nodes[id]):
    return db.nodes[id]
  return nil

proc exists*(db: FlatDb, id: EntryId): bool =
  ## returns true if entry with given EntryId exists
  return db.nodes.hasKey(id)

proc exists*(db: FlatDb, matcher: Matcher): bool =
  ## returns true if we found at least one match
  return (not queryOne(db, matcher).isNil)

proc notExists*(db: FlatDb, matcher: Matcher): bool =
  ## returns false if we found no match
  return not db.exists(matcher)

# ----------------------------- Matcher -----------------------------------------
proc equal*(key: string, val: string): Matcher {.inline.} =
  return proc (x: JsonNode): bool =
    return x.getOrDefault(key).getStr() == val
proc equal*(key: string, val: int): Matcher {.inline.} =
  return proc (x: JsonNode): bool  =
    return x.getOrDefault(key).getInt() == val
proc equal*(key: string, val: float): Matcher {.inline.} =
  return proc (x: JsonNode): bool  =
    return x.getOrDefault(key).getFloat() == val
proc equal*(key: string, val: bool): Matcher {.inline.} =
  return proc (x: JsonNode): bool  =
    return x.getOrDefault(key).getBool() == val

proc lower*(key: string, val: int): Matcher {.inline.} =
  return proc (x: JsonNode): bool = x.getOrDefault(key).getInt < val
proc lower*(key: string, val: float): Matcher {.inline.} =
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFloat < val
proc lowerEqual*(key: string, val: int): Matcher {.inline.} =
  return proc (x: JsonNode): bool = x.getOrDefault(key).getInt <= val
proc lowerEqual*(key: string, val: float): Matcher {.inline.} =
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFloat <= val

proc higher*(key: string, val: int): Matcher {.inline.} =
  return proc (x: JsonNode): bool = x.getOrDefault(key).getInt > val
proc higher*(key: string, val: float): Matcher {.inline.} =
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFloat > val
proc higherEqual*(key: string, val: int): Matcher {.inline.} =
  return proc (x: JsonNode): bool = x.getOrDefault(key).getInt >= val
proc higherEqual*(key: string, val: float): Matcher {.inline.} =
  return proc (x: JsonNode): bool = x.getOrDefault(key).getFloat >= val

proc dbcontains*(key: string, val: string): Matcher {.inline.} =
  return proc (x: JsonNode): bool =
    let str = x.getOrDefault(key).getStr()
    return str.contains(val)
proc dbcontainsInsensitive*(key: string, val: string): Matcher {.inline.} =
  return proc (x: JsonNode): bool =
    let str = x.getOrDefault(key).getStr()
    return str.toLowerAscii().contains(val.toLowerAscii())

proc between*(key: string, fromVal:float, toVal: float): Matcher {.inline.} =
  return proc (x: JsonNode): bool =
    let val = x.getOrDefault(key).getFloat
    val > fromVal and val < toVal
proc between*(key: string, fromVal:int, toVal: int): Matcher {.inline.} =
  return proc (x: JsonNode): bool =
    let val = x.getOrDefault(key).getInt
    val > fromVal and val < toVal
proc betweenEqual*(key: string, fromVal:float, toVal: float): Matcher {.inline.} =
  return proc (x: JsonNode): bool =
    let val = x.getOrDefault(key).getFloat
    val >= fromVal and val <= toVal
proc betweenEqual*(key: string, fromVal:int, toVal: int): Matcher {.inline.} =
  return proc (x: JsonNode): bool =
    let val = x.getOrDefault(key).getInt
    val >= fromVal and val <= toVal

proc has*(key: string): Matcher {.inline.} =
  return proc (x: JsonNode): bool = return x.hasKey(key)

proc `and`*(p1, p2: proc (x: JsonNode): bool): Matcher {.inline.} =
  return proc (x: JsonNode): bool = return p1(x) and p2(x)

proc `or`*(p1, p2: proc (x: JsonNode): bool): Matcher {.inline.} =
  return proc (x: JsonNode): bool = return p1(x) or p2(x)

proc `not`*(p1: proc (x: JsonNode): bool): Matcher {.inline.} =
  return proc (x: JsonNode): bool = return not p1(x)

proc close*(db: FlatDb) =
  db.stream.flush()
  db.stream.close()

proc keepIf*(db: FlatDb, matcher: proc) =
  ## filters the database file, only lines that match `matcher`
  ## will be in the new file.
  # TODO 
  db.store db.query matcher

proc delete*(db: FlatDb, id: EntryId, doFlush = true) =
  ## deletes entry by id
  var hit = false
  if db.nodes.hasKey(id):
      hit = true
      db.nodes.del(id)
  if doFlush and hit:
    db.flush()

template deleteImpl(db: FlatDb, direction: untyped, matcher: Matcher, doFlush = true) = 
  var hit = false
  for item in direction(matcher):
    hit = true
    db.nodes.del(item["_id"].getStr)
  if doFlush and hit:
    db.flush()
template delete*(db: FlatDb, matcher: Matcher, doFlush = true) =
  ## deletes entry by matcher, respects `manualFlush`
  deleteImpl(db, db.queryIter, matcher, doFlush)
template deleteReverse*(db: FlatDb, matcher: Matcher, doFlush = true) =
  ## deletes entry by matcher, respects `manualFlush`
  deleteImpl(db, db.queryIterReverse, matcher, doFlush)

proc upsert*(db: FlatDb, node: JsonNode, eid: EntryId = "", doFlush = true): EntryId {.discardable.} =
  ## inserts or updates an entry by its entryid, if flush == true db gets flushed
  if eid == "" or (not db.exists(eid)):
    return db.append(node, eid, doFlush)
  else:
    db.update(eid, node, doFlush)
    return eid

proc upsert*(db: FlatDb, node: JsonNode, matcher: Matcher, doFlush = true): EntryId {.discardable.} =
  let entry = db.queryOne(matcher)
  if entry.isNil:
    return db.append(node, doFlush = doFlush)
  else:
    db[entry["_id"].getStr] = node
    return entry["_id"].getStr

# TODO ?
# proc upsertMany*(db: FlatDb, node: JsonNode, matcher: Matcher, flush = db.manualFlush): EntryId {.discardable.} = 
  ## updates entries by a matcher, if none was found insert new entry
  ## if flush == true db gets flushed
