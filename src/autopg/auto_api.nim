import os, httpcore, json, db_common, tables, strutils, db_postgres, strformat
import auto_pg 
import readschema

import noah / webcontext

proc auto_api*(db: DbConn, ctx: WebContext): WebContext =
  ## Given a WebContext and a Postgres DB Connection, creates an
  ## automatic RERT API over each table in the public schema
  ## `/schema`  returns the actual schema of the database
  let headers = newHttpHeaders([("Content-Type","application/json")])
  result = ctx.copy()
  #let dbname = os.getEnv("DATABASE_NAME")
  let schema = db.get_tables()
  var tables: seq[string] = @[]
  for t in schema:
    echo "table: " & t.name
    tables.add(t.name)
  echo "\ntables: " & $tables & "\n====\n"
  let tname = ctx.request.urlpath[1]
  echo "TNAME = " & tname
  if tname == "schema":
    let schema = db.readTables().toJson()
    #echo schema.pretty()
    result.response.body  = $schema
    result.response.headers = headers
    result.response.status = Http200
    return
  if tname in tables:
    case ctx.request.reqMethod:
      of HttpGet:
        echo "\nGET: " & ctx.request.body
        result.response.body = $db.get_data(tname, ctx)
        result.response.status = Http200
        result.response.headers = headers
      of HttpPost:
        result.response.body = $db.post_data(tname, parseJson(ctx.request.body))
        result.response.status = Http200
        result.response.headers = headers
      of HttpPut:
        echo "\nPUT: " & ctx.request.body
        let res = db.put_data(tname, parseJson(ctx.request.body))
        echo "--- finished"
        echo $res
        result.response.body = $res
        result.response.status = Http200
        result.response.headers = headers
      of HttpDelete:
        result.response.body = $db.delete_data(tname, ctx)
        result.response.status = Http200
        result.response.headers = headers
      else:
        result.response.body = """{"error":"Method not found"}"""
        result.response.status = Http404
        result.response.headers = headers
  else:
    echo "404" & $result
    result.response.body = """{"status":"OK", "code": 404, "message":"Not found"}"""
    result.response.status = Http404
    result.response.headers = headers

