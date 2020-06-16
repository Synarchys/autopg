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
    precision*: int
    length*: int
  DBTable* = object
    ## Simplest table schema representation
    name*: string 
    columns*: seq[DBColumn]
    
proc get_columns*(pg:DbConn, db_table: string): seq[DBColumn] =
  ## Gets all columns from a given table
  result = @[]
  let rows = pg.getAllRows(sql"""
     select
      column_name, data_type, numeric_precision, character_maximum_length
     from
       information_schema.columns
     where
       table_catalog = current_database()
       and table_name = ?
       and table_schema = 'public'
    """, db_table)
  for r in rows:
    var column = DBColumn(name:r[0], ctype:r[1])
    case column.ctype
    of "text", "character varying", "varchar":
      if r[3] != "":
        column.length = parseInt r[3]
    of "numeric", "double precision":
      if r[2] != "":
        column.precision = parseInt r[2]
    #, precision: r[2], length: r[3]
    result.add(column)
  
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
  
proc get_data*(pg:DbConn, db_table: string, ctx: WebContext = nil): JsonNode =
  ## Given a table and a query, returns a JsonNode containing the result.
  ##
  ## Only very rigid '=' queries are allowed now an\d can couse exceptios if
  ## the listed fields do not exist or are of a different type
  let db_schema = pg.get_tables()
  var whereClause = ""
  
  if ctx != nil and ctx.request.paramTable.len > 0 :
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

proc extractJSonVal(c: DbColumn, item: JsonNode): string =
    case c.ctype:
    of "int", "integer", "bigint", "smallint":
      result = $item[c.name].getInt()
    of "boolean":
      result = $item[c.name].getBool()
    of "numeric", "double precision":
      result = $item[c.name].getFloat
    else:
      result = dbQuote(item[c.name].getStr())
               
proc genSQLValue(c: DbColumn, item: JsonNode): string =
  result = extractJSonVal(c, item) & ", "
                     
proc post_data*(pg:DbConn, db_table: string, d: JsonNode, insertNull = true): JsonNode =
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
      var insertColumns: seq[DBColumn] # the columns to be inserted
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
          if insertNull and (not item.haskey(c.name) or item[c.name].kind == JNull):
            values = values & " null " & ", "
            if not insertColumns.contains c:
              insertColumns.add c
            
          elif item.haskey(c.name):
            if not insertColumns.contains c:
              insertColumns.add c              
            values = values & genSQLValue(c, item)

        values.delete(values.len - 2, values.len)
        values = values & " ), "
      values.delete(values.len - 2, values.len)  
      var statement = "INSERT INTO " & db_table & " ("
      for c in insertColumns:
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
        var
          setClause: string 
          statement: string
          whereStmt = " WHERE id = "
        
        for item in data:
          setClause = ""
          statement = ""
          for c in columns:
            # echo "Column name: " & c.name & " || type: " & c.ctype
            var qt = "'"
            if item.haskey(c.name) and item[c.name].kind == JNull:
              # sets to NULL if the column name is passed and its value is JNull
              # used to delete the value of a column
              setClause = setClause & c.name & " =  null " & ", "
            elif item.haskey(c.name):
              setClause = setClause & c.name & " = "
              setClause = setClause & genSQLValue(c, item)
            if c.name == "id":
              # if there's no id there be no `where clause`, use pk from schema?
              whereStmt =  whereStmt & extractJSonVal(c, item)

          let lastComma = setClause.rfind(",")
          setClause.delete(lastComma, lastComma + 1)
          var statement = "UPDATE " & db_table & " SET " & setClause & whereStmt
          result = %*{ "updated": data.len}
    else:
      result = %*{ "error": "invalid format"}
  except:
    echo "==> ERROR: " & getCurrentExceptionMsg()
    result = %*{ "error": getCurrentExceptionMsg()}

