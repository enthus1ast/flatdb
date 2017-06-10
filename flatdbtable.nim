## flat db table 
## double linked list with 'n' hashset indexes on the values
# import sets
import lists
import json
import tables
import oids

type 
  # FlatDbTableNode = object of DoublyLinkedNode
  #   key*: string

  Entry = tuple[key:string, value: JsonNode]
  FlatDbTableData = DoublyLinkedList[Entry]
  FlatDbTableIndex = Table[string, DoublyLinkedNode[Entry]]
  FlatDbTableId = string

  FlatDbTable* = ref object
    data: FlatDbTableData
    index: FlatDbTableIndex
    size: int

proc newFlatDbTable*(): FlatDbTable = 
  result = FlatDbTable()
  result.data = initDoublyLinkedList[Entry]()
  result.size = 0

  # result.index = FlatDbTableIndex()
  result.index = initTable[string, DoublyLinkedNode[Entry]]()

# proc insert*(table: FlatDbTable, pos: idx, key: string, value: JsonNode) = 
#   table.data.insert()

proc add*(table: FlatDbTable, key: string,  value: JsonNode) = #: FlatDbTableId = 
  table.data.append((key, value))
  table.index.add(key, table.data.tail)  
  table.size.inc

proc `[]`*(table: FlatDbTable, key: string): JsonNode = 
  return table.index[key].value[1] 

proc `[]=`(table: FlatDbTable, key: string, value: JsonNode) =
   table.index[key].value.value = value

proc hasKey*(table: FlatDbTable, key: string): bool =
  ## returns true if table has a items with key == key
  return table.index.hasKey(key)

# template getOrDefault(table, key): JsonNode = 
#   table.index.getOrDefault(key)

proc del*(table: FlatDbTable, key: string) =
  var it = table.index[key]
  table.index.del(key)
  table.data.remove(it)
  table.size.dec

proc clear*(table: FlatDbTable) = 
  table.index.clear()
  # table.data.remove()
  # TODO clear linked list better? 
  for it in table.data.nodes:
    table.data.remove(it)
  table.size = 0

proc len*(table: FlatDbTable): int =
  return table.size

iterator values*(table: FlatDbTable): JsonNode =
  var it = table.data.head
  while it != nil:
    yield it.value.value
    it = it.next

iterator valuesReverse*(table: FlatDbTable): JsonNode =
  # for elem in table.data.items.tail:
    # yield elem    
  var it = table.data.tail
  while it != nil:
    yield it.value.value
    it = it.prev

iterator pairs*(table: FlatDbTable): Entry = # id entry
  var it = table.data.head
  while it != nil:
    yield it.value
    it = it.next

iterator pairsReverse*(table: FlatDbTable): Entry = # id entry
  var it = table.data.tail
  while it != nil:
    yield it.value
    it = it.prev

when isMainModule:
  import sequtils
  block:
    var t1 = %* {"foo": 1}
    var t2 = %* {"foo": 2}
    var table = newFlatDbTable()
    table.add("id1", t1)
    table.add("id2", t2)

    assert table["id1"] == t1
    assert table["id2"] == t2


    assert table["id1"].getOrDefault("foo").getNum() == 1

    assert toSeq(table.values) == @[t1,t2]
    assert toSeq(table.valuesReverse) == @[t2, t1]

    assert toSeq(table.pairs) == @[("id1", t1), ("id2", t2)]
    assert toSeq(table.pairsReverse) ==  @[("id2", t2),("id1", t1)]

    assert table.len == 2
    table.del("id2")
    assert toSeq(table.values) == @[t1]
    assert toSeq(table.valuesReverse) == @[t1]
    assert table.len == 1

  block: # clear
    var t1 = %* {"foo": 1}
    var t2 = %* {"foo": 2}
    var table = newFlatDbTable()
    table.add("id1", t1)
    table.add("id2", t2)
    table.clear()
    assert table.len == 0  
    assert toSeq(table.values) == @[]
    assert toSeq(table.valuesReverse) == @[] 

  block: # change/update
    var t1 = %* {"foo": 1}
    var t2 = %* {"foo": 2}
    var table = newFlatDbTable()
    table.add("id1", t1)  
    table.add("id2", t2)

    let entry = table["id1"]
    entry["foo"] = % "klaus"
    assert toSeq(table.pairs) == @[("id1", %* {"foo": "klaus"} ), ("id2", t2)]

    table["id2"] = %* {"klaus": "klauspeter"}
    assert toSeq(table.pairs) == @[("id1", %* {"foo": "klaus"} ), ("id2",  %* {"klaus": "klauspeter"} )]

  block:
    var table = newFlatDbTable()

    for idx in 0..10_000:
      var t1 = %* {"foo": idx}  
      table.add($idx,t1)

    for each in table.valuesReverse():
      echo each

