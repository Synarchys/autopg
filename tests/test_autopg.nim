
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
      "col1": "second column",
      "col2": true,
      "col3": d
    }
    
  # Insert data
  echo "Insert"
  let rInsert = db.post_data(tableName, %*{"data": [item]})
  doAssert($rInsert == """{"inserted":1}""")

  # Retrieve data
  let rGet = db.get_data(tableName)
  doAssert($rGet == $(%*{"test_autopg":[item]}), "Get did not match expected result.")
  
  # Update Data
  echo "Update"
  var item1 = item.copy()
  let d1 = format(now(), f)

  item1["col1"] = %"updated value"
  item1["col2"] = %false
  item1["col3"] = %d1
  
  let
    rUpdate = db.put_data(tableName, %*{"data": [item1]})
    rUpdateGet = db.get_data(tableName)

  echo rUpdateGet
  doAssert($rUpdate == """{"updated":1}""")
  doAssert($rUpdateGet == $(%*{"test_autopg":[item1]}), "Update did not match expected result.")
  
  # Upsert data
  var item2 = item1.copy()
  item2["id"] = %2
  item2["col1"] = %"upserted inserted value"
  item2["col2"] = %false
  
  let
    upserted = db.upsert_data(tableName, %*{"data": [item2]})
    getUpserted = db.get_data(tableName)

  echo getUpserted
  doAssert($upserted == """{"upserted":1}""")
  
  item2["col1"] = %"upserted updated value"
  item2["col2"] = %true

  let
    upserted1 = db.upsert_data(tableName, %*{"data": [item2]})
    getUpserted1 = db.get_data(tableName)

  echo getUpserted1
  doAssert($upserted == """{"upserted":1}""")
   
except Exception as e:
  #raise e
  echo e.msg
  
finally:
  echo "Dropping table."
  # drop table
  db.exec(sql"drop table public.test_autopg")
  db.close()
