
import db_postgres, times, json, strutils
import asynchttpserver, uri
import noah / asynccontext
import ../src/autopg

const
  user = "postgres"
  password = "postgres"
  dbname = "postgres"
  tableName = "test_autopg"

let
  db = open("localhost", user, password, dbname)
  f = initTimeFormat("yyyy-MM-dd\'T\'HH:mm:ss")

try:
  # Create table
  const createTableSql =
    """CREATE TABLE IF NOT EXISTS public.test_autopg
      (
        id integer primary key,
        col1 character varying,
        col2 boolean,
        col3 timestamp without time zone
      );
    """
  db.exec(sql createTableSql)

  let
    d = format(now(), f)
    item = %*{
      "id": 1,
      "col1": "item 1",
      "col2": true,
      "col3": d
    }
    item0 = %*{
      "id": 2,
      "col1": "item 2",
      "col2": false,
      "col3": d
    }
    
  # Insert data
  echo "Insert"
  let rInsert = db.post_data(tableName, %*{"data": [item, item0]})
  doAssert($rInsert == """{"inserted":2}""")

  # Retrieve data
  let rGet = db.get_data(tableName)
  doAssert($rGet == $(%*{"test_autopg":[item, item0]}), "Get did not match expected result.")
  
  # Update Data
  echo "Update"
  var item1 = item.copy()
  let d1 = format(now(), f)
  item1["col1"] = %"updated item 1"
  item1["col2"] = %false
  item1["col3"] = %d1

  var item2 = item0.copy()
  item2["col1"] = %"updated item 2"
  item2["col2"] = %true
  item2["col3"] = %d1

  let
    rUpdate = db.put_data(tableName, %*{"data": [item1, item2]})
    rUpdateGet = db.get_data(tableName)

  doAssert($rUpdate == """{"updated":2}""")
  doAssert($rUpdateGet == $(%*{"test_autopg":[item1, item2]}), "Update did not match expected result.")
  
  # Upsert data do nothing
  var item3 = item1.copy()
  item3["id"] = %2
  item3["col1"] = %"upserted inserted value"
  item3["col2"] = %false

  # inserts new data
  let
    upserted = db.upsert_data(tableName, %*{"data": [item3]})
    getUpserted = db.get_data(tableName)

  echo getUpserted
  doAssert($upserted == """{"upserted":1}""")
  
  item2["col1"] = %"upserted updated value"
  item2["col2"] = %true
  
  # on conflict does nothing
  let
    upserted1 = db.upsert_data(tableName, %*{"data": [item3]})
    getUpserted1 = db.get_data(tableName)
    
  echo getUpserted1
  doAssert($upserted1 == """{"upserted":1}""")

  # on conflict updates
  let
    upserted2 = db.upsert_data(tableName, %*{"data": [item3]}, onConflict=OnConflict.update)
    getUpserted2 = db.get_data(tableName)

  echo getUpserted2
  doAssert($upserted2 == """{"upserted":1}""")

   
except Exception as e:
  echo e.msg
  raise e
  
finally:
  echo "Dropping table."
  # drop table
  db.exec(sql"drop table public.test_autopg")
  db.close()
