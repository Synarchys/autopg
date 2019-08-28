## Postgres DB driver extensions for auto generated API
## 
import db_postgres,
       os,
       json,
       times,
       strutils,
       tables,
       uri,
       sequtils,
       strformat,
       sugar   

import noah / webcontext 

type
  DBColumn* = object
    ## Simplest schema column representation
    name*: string
    ctype*: string
  DBTable* = object
    ## Simplest table schema representation
    name*: string 
    columns*: seq[DBColumn]
    

proc get_columns*(pg:DbConn, db_table: string): seq[DBColumn] =
  ## Gets all columns from a given table
  result = @[]
  let rows = pg.getAllRows(sql"""
     select
      column_name, data_type
     from
       information_schema.columns
     where
       table_catalog = current_database()
       and table_name = ?
       and table_schema = 'public'
    """, db_table)
  for r in rows:
    result.add(DBColumn(name:r[0], ctype:r[1]))
  
proc get_tables*(pg:DbConn): seq[DBTable] =
  result = @[]
  let rows = pg.getAllRows(sql"""
   select
    table_name
   from
     information_schema.tables
   where
     table_catalog = current_database()
     and table_schema = 'public'
     and table_type = 'BASE TABLE';
  """)
  for r in rows:
    let cols = pg.get_columns(r[0])
    result.add(DBTable(name:r[0], columns:cols))

proc get_schema*(pg: DBConn): JsonNode =
  ## Return a JsonNode representing the DB schema
  let tables = pg.get_tables()
  result = %* tables
  

proc get_data*(pg:DbConn, db_table: string, ctx: WebContext): JsonNode =
  ## Given a table and a query, returns a JsonNode containing the result.
  ##
  ## Only very rigid '=' queries are allowed now an\d can couse exceptios if
  ## the listed fields do not exist or are of a different type
  let db_schema = pg.get_tables()
  var whereClause = ""
  echo ctx
  if ctx.request.paramTable.len > 0 :
    let tables = db_schema.filter do (t:DBTable) -> bool : t.name == db_table
    var columns: seq[DBColumn]
    if tables.len > 0:
      columns = tables[0].columns
    else:
      result = %*{"error_message":"invalid table"}
      return
    # knoledge date default is now()
    # observation date default is now()
    let columnNames = columns.map do (c: DBColumn) -> string : c.name
    whereClause = " where "
    for key, value in ctx.request.paramTable:
      if key in columnNames: 
        let val = decodeUrl(value)
        whereClause.add(key & " = '" & val & "' and ")
    whereClause.delete(whereClause.len - 4, whereClause.len - 1)
  var statement = "select to_json(k) from (select array_to_json(array_agg(row_to_json(j))) as " & db_table & " from (select * from " & db_table & whereClause & " ) j) k"
  let rows = pg.getAllRows(sql(statement))
  if rows[0].len > 0:
    result = parseJson($rows[0][0])
  else:
    result = %*{"message": "No rows found"}
  
proc post_data*(pg:DbConn, db_table: string, d: JsonNode): JsonNode =
  ## Inserts the contents of a JsonNode into a table. the format of the Json
  ## data is:
  ##
  ##
  ## {"data": [ {<data item>}, {<data item>}, ...] }
  ##
  ## Fields not present in the data item are leaved for the table's default
  ##
  ## Fields not existing in the database are silently ignored
  echo "\n\n---------\nDATA:" & $d
  let db_schema = pg.get_tables()
  if d.kind == JObject:
    for k, v in d:
      let data = v
      let tables = db_schema.filter do (t:DBTable) -> bool : t.name == db_table
      var columns: seq[DBColumn]
      if tables.len > 0:
        columns = tables[0].columns
      else:
        result = %*{"error_message":"invalid table"}
      var values = ""
      for item in data:
        values = values & " ("
        for c in columns:
          var qt = "'"
          if not item.haskey(c.name) or item[c.name].kind == JNull:
            values = values & " null " & ", "
          else: 
            case c.ctype:
              of "int":
                values = values & $item[c.name].getInt() & ", "
              of "smallint":
                values = values & $item[c.name].getInt() & ", "
              of "boolean":
                values = values & $item[c.name].getBool() & ", "
              of "numeric":
                values = values & $item[c.name].getFloat & ", "
              else: 
                values = values & qt & item[c.name].getStr() & qt & ", "
        values.delete(values.len - 2, values.len)
        values = values & " ), "
      values.delete(values.len - 2, values.len)  
      var statement = "INSERT INTO " & db_table & " ("
      for c in columns:
        statement = statement & "\"" & c.name & "\"" & ", "
      statement.delete(statement.len - 2, statement.len)
      statement = statement & ") VALUES " & values
      echo "SQL:" & $statement & "\n-----------\n" 

      pg.exec(sql(statement))
      echo "executed"
      result = %*{ "inserted": data.len}

proc delete_data*(pg:DbConn, db_table: string, ctx: WebContext): JsonNode =
  ## Deletes rows from a table given a list of ids as parameters
  ##
  ## CAUTION!: if no id is given, deletes all rows from the table 
  var statement = "DELETE FROM " & db_table 
  var whereStmt = ""
  if ctx.request.paramList.len > 0:
    statement.add " WHERE id IN ("
    for id in ctx.request.paramList:
      statement.add dbQuote(id) & ", "
    statement.delete(statement.len - 1 , statement.len)
    statement.add ")"
  pg.exec(sql(statement))
  result = %*{"deleted": ctx.request.paramList.len}


proc put_data*(pg:DbConn, db_table: string, d: JsonNode): JsonNode =
  ## Updates the contents of a JsonNode into a table. the format of the Json
  ## data is:
  ##
  ##
  ## {"data": [ {<data item>}, {<data item>}, ...] }
  ##
  ## Fields not present in the data item are leaved for the table's default
  ##
  ## Fields not existing in the database are silently ignored
  let db_schema = pg.get_tables()
  try:
    if d.kind == JObject:
      for k, v in d:
        let data = v
        let tables = db_schema.filter do (t:DBTable) -> bool : t.name == db_table
        var columns: seq[DBColumn]
        if tables.len > 0:
          columns = tables[0].columns
        else:
          result = %*{"error_message":"invalid table"}
        var setClause: string 
        var statement: string
        for item in data:
          setClause = ""
          statement = ""
          for c in columns:
            echo "Column name: " & c.name & " || type: " & c.ctype
            var qt = "'"
            if not item.haskey(c.name) or item[c.name].kind == JNull:
              #setClause = setClause & c.name & " =  null " & ", "
              discard
            else:
              setClause = setClause & c.name & " = " 
              case c.ctype:
                of "int":
                  setClause = setClause & $item[c.name].getInt() & ", "
                of "smallint":
                  setClause = setClause & $item[c.name].getInt() & ", "
                of "boolean":
                  setClause = setClause & $item[c.name].getBool() & ", "
                of "numeric":
                  setClause = setClause & $item[c.name].getFloat & ", "
                else: 
                  setClause = setClause & dbQuote(item[c.name].getStr()) & ", "

          let lastComma = setClause.rfind(",")
          setClause.delete(lastComma, lastComma + 1)
          var statement = "UPDATE " & db_table & " SET " & setClause &
            " WHERE id =  " & dbQuote(item["id"].getStr())
          
          pg.exec(sql(statement))
          result = %*{ "updated": data.len}
    else:
      result = %*{ "error": "invalid format"}
  except:
    echo "==> ERROR: " & getCurrentExceptionMsg()
    result = %*{ "error": getCurrentExceptionMsg()}

