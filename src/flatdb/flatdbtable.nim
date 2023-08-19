#
#
#                     flatdb
#        (c) Copyright 2023 David Krause
#
#    See the file "licence", included in this
#    distribution, for details about the copyright.

## flat db table 
## double linked list with table indexes on the values
import lists, json, tables, oids

type 
  Node* = DoublyLinkedNode[Entry]
  Entry = tuple[key:string, value: JsonNode]
  FlatDbTableData = DoublyLinkedList[Entry]
  FlatDbTableIndex = Table[string, Node]
  FlatDbTableId = string
  FlatDbTable* = ref object
    data: FlatDbTableData
    index: FlatDbTableIndex
    size: int

proc newFlatDbTable*(): FlatDbTable =
  result = FlatDbTable()
  result.data = initDoublyLinkedList[Entry]()
  result.size = 0
  result.index = initTable[string, Node]()

proc add*(table: FlatDbTable, key: string,  value: JsonNode) = #: FlatDbTableId =
  table.data.append((key, value))
  table.index.add(key, table.data.tail)
  table.size.inc

proc `[]`*(table: FlatDbTable, key: string): JsonNode =
  return table.index[key].value[1]

proc `[]=`*(table: FlatDbTable, key: string, value: JsonNode) =
   table.index[key].value.value = value

proc getNode*(table: FlatDbTable, key: string): Node =
  ## retrieves the `Node` by id
  ## this is the doubly linked list entry, which exposes next and prev!
  return table.index[key]
  
proc hasKey*(table: FlatDbTable, key: string): bool =
  ## returns true if table has a items with key == key
  return table.index.hasKey(key)

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
  var it = table.data.tail
  while it != nil:
    yield it.value.value
    it = it.prev

iterator pairs*(table: FlatDbTable): Entry =
  var it = table.data.head
  while it != nil:
    yield it.value
    it = it.next

iterator pairsReverse*(table: FlatDbTable): Entry =
  var it = table.data.tail
  while it != nil:
    yield it.value
    it = it.prev

