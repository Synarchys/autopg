
import strutils, sequtils, strformat, db_postgres, sugar, os, parseutils, json

import unicode
import ../ autopg / auto_pg
import db_postgres


proc usage*() =
  echo "Usage: "
  echo "\tautopg_tools -o:<dir> <dburl>"
  echo "Example:"
  echo "\tautopg_tools -o:mydir/ postgresql://user:secret@localhost/mydb"
  quit()


proc toNimType(tname: string): string =
  case tname:
    of "uuid":
      result = "string"
    of "bool", "boolean":
      result = "bool"
    of "text", "character varying", "varchar", "character", "char":
      result = "string"
    of "integer", "numeric", "double precision":
      result = "int64"
    of "smallint":
      result = "int16"
    of "timestamp", "timestamp without time zone", "timestamp with time zone":
      result = "Time"
    of "date", "datetime" :
      result = "DateTime"
    else:
      result = "Type " & tname & " Not Found"
      echo "======================================================="
      echo result
      echo "======================================================="
    
proc genSchemaFile*(dir, dburl: string) =
  if dir == "" or dburl == "":
    usage()
    
  let
    db = open("", "", "", dburl) #open("host", "user", "passwd", "dbname")
    filename = dir & "/auto_model.nim"
  echo "------------------------------------------------"
  echo fmt"Generating model in {filename}"
  var f = open(filename, fmWrite)
  
  f.writeLine "import times, json"
  f.writeLine ""
  f.writeLine "type"
  let tables = db.get_tables()
  for t in tables:
    let typeName = t.name
    echo fmt"  Generating type for table {t.name}"
    echo "  Type " & typeName
    f.writeLine fmt"  {typeName}* = ref object"
    let columns = db.get_columns(t.name)
    for c in columns:
      let cName = c.name
      f.writeLine fmt"    {cName}*: {c.ctype.toNimType()}"
    f.writeLine ""

  echo fmt"{filename} genetared..."
  echo "------------------------------------------------"

  
