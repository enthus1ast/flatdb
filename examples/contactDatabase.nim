import ../src/flatdb
import json
import tables
import os
# A simple contacts database example


var contacts = newFlatDb("contacts.db", false)
discard contacts.load() # we load all the data from file

if contacts.nodes.len == 0: # if no data there we create some testdata.
  discard contacts.append(%* {"name": "Klaus", "phone": "012 8378 89", "country": "Germany", "email": "Klaus@example.org", "someUniqueKey": "foo"})
  discard contacts.append(%* {"name": "Max Musterman", "phone": "787 78 98", "country": "Germany", "email": "Max.Musterman@example.org"})
  discard contacts.append(%* {"name": "Peter", "phone": "388983 89 89", "country": "Austria", "email": "Peter@example.org"})
  discard contacts.append(%* {"name": "Hans", "phone": "889 8 98 89", "country": "Netherlands", "email": "Hans@example.org"})
  discard contacts.append(%* {"name": "Иван Петрович Сидоров", "phone": "6 76 878 9", "country": "Russia", "email": "ИванПетровичСидоров@example.org"})
  discard contacts.append(%* {"name": "王秀英", "phone": "1231236 71231326 878 9", "country": "China", "email": "王秀英@example.org"})
  discard contacts.append(%* {"name": "张丽", "phone": "123 8713238 9", "country": "China", "email": "张丽@example.org"})
  discard contacts.append(%* {"name": "John Doe", "phone": "6123 1376 837812 39", "country": "Usa", "email": "John.Doe@example.org"})


echo contacts.query equal("name", "Klaus")
echo contacts.query has("someUniqueKey") or equal("country", "Netherlands")
echo contacts.query equal("name", "Иван Петрович Сидоров")


removeFile("./contacts.db") # to cleanup after this example