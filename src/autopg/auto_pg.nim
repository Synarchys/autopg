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

proc get_primary_keys*(pg: DBConn): Table[string, string] =
  # returns table name and column name
  # currently using only public schema
  result = initTable[string, string]()
  let rows = pg.getAllRows(
    sql"""select tc.table_schema, tc.table_name, kc.column_name
     from
       information_schema.table_constraints tc,
       information_schema.key_column_usage kc
     where
       tc.constraint_type = 'PRIMARY KEY'
       and kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
       and kc.constraint_name = tc.constraint_name
     order by 1, 2""")

  for r in rows:
    if r[0] == "public":
      result[r[1]] = r[2]
  
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

proc genInsertStmt(pg:DbConn, db_table: string, d: JsonNode, insertNull = true): (int, string) =
  ## Generates a sql statement for insertion
  ## data is:
  ## {"data": [ {<data item>}, {<data item>}, ...] }
  ##
  ## Fields not present in the data item are leaved for the table's default
  ##
  ## Fields not existing in the database are silently ignored
  ## Returns statment, affected rows  
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
        # TODO
        #result = %*{"error_message":"invalid table"}
        result = (-1, "invalid table")
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
      result = (data.len, statement)
      
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
  let query = genInsertStmt(pg, db_table, d, insertNull)
  if query[0] > -1:
    pg.exec(sql(query[1]))
    result = %*{ "inserted": query[0]}
  else:
    result = %*{"error_message": query[1]}
               
proc genSetStmt(pg:DbConn, db_table: string, d: JsonNode, genWhere: bool = true): (int, string) =
  let
    db_schema = pg.get_tables()

  var
    pks: Table[string, string]
    pk: string
      
  if genWhere:
      pks = get_primary_keys(pg)
      pk = pks[db_table]
  
  if d.kind == JObject:
    for k, v in d:
      let data = v
      let tables = db_schema.filter do (t:DBTable) -> bool : t.name == db_table
      var columns: seq[DBColumn]
      if tables.len > 0:
        columns = tables[0].columns
      else:
        result = (-1,"invalid table")
      var
        setClause: string 
        statement: string
        whereStmt: string        
      for item in data:
        setClause = ""
        statement = ""
        whereStmt = ""        
        for c in columns:
          # var qt = "'"
          if item.haskey(c.name) and item[c.name].kind == JNull:
            # sets to NULL if the column name is passed and its value is JNull
            # used to delete the value of a column
            setClause = setClause & c.name & " =  null " & ", "
          elif item.haskey(c.name):
            setClause = setClause & c.name & " = "
            setClause = setClause & genSQLValue(c, item)
          if genWhere and pk != "" and c.name == pk:
            whereStmt =  " WHERE " & c.name & " = " & extractJSonVal(c, item)

        let lastComma = setClause.rfind(",")
        setClause.delete(lastComma, lastComma + 1)
        # var statement = "UPDATE " & db_table & " SET " & setClause & whereStmt
        let statement = "SET " & setClause & whereStmt
        result = (data.len, statement)
    
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
  try:
    let query = genSetStmt(pg, db_table, d)
    if query[0] > -1:
      let stmt = "UPDATE " & db_table & & " " & query[1]
      pg.exec(sql(stmt))
      result = %*{ "updated": query[0]}
    else:
      result = %*{"error_message": query[1]}
  except:
    let e = getCurrentException()
    echo e.getStackTrace()
    echo "==> ERROR: " & getCurrentExceptionMsg()
    result = %*{ "error": getCurrentExceptionMsg()}

proc upsert_data*(pg:DbConn, db_table: string, d: JsonNode): JsonNode =
  # upserts on conflict on primary key
  let 
    pks = get_primary_keys(pg)
    pk = pks[db_table]
    insert = genInsertStmt(pg, db_table, d, insertNull = false)
    setStmt = genSetStmt(pg, db_table, d, genWhere = false)
    query = insert[1] & " ON CONFLICT (" & pk & ") DO UPDATE " & setStmt[1]

  pg.exec(sql(query))
  result = %*{ "upserted": insert[0]}
                 
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
