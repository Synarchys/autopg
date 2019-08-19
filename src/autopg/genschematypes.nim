
import strutils, sequtils, strformat, db_postgres, sugar, os, parseutils, json
      
import autopg / auto_pg
import db_postgres


proc usage() =
  echo "Usage: "
  echo "genschematypes -o:<dir> <dburl>"
  echo "Example"
  echo "genschematypes -o: mydir/ postgresql://user:secret@localhost"
  quit()


proc toNimType(tname: string): string =
  case tname:
    of "uuid": result = "string"
    of "bool", "boolean": result = "bool"
    of "text": result = "string"
    of "integer": result = "int64"
    of "smallint": result = "int16"
    else:
      result = "=========== " & tname
  if tname.find("varchar") >= 0:
    result = "string"
  if tname.find("numeric") >= 0:
    result = "float64"
  if tname.find("char") >= 0:
    result = "string"
  if tname.contains("timestamp") or tname.contains("date") :
    result = "DateTime"


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
    echo fmt"  Generating type for {t.name}"
    f.writeLine fmt"  {t.name.capitalizeAscii()}* = ref object"
    let columns = db.get_columns(t.name)
    for c in columns:
      #if c.name == t.name:
      f.writeLine fmt"    {c.name}* : {c.ctype.toNimType()}"
      f.writeLine ""
  echo fmt"{filename} genetared..."
  echo "------------------------------------------------"

  

# when declared(commandLineParams):
#   var
#     dir: string
#     dburl: string
#   let params = commandLineParams()
#   if params.len <= 0:
#     usage()  
    
#   for param in params:
#     if param.startsWith "-o:":
#       dir = param.split(":")[1]
#     elif param != "":
#       dburl = param
#   genSchemaFile(dir, dburl)
