
import os, strutils
import src / autopg / genschematypes


when declared(commandLineParams):
  var
    dir: string
    dburl: string
  let params = commandLineParams()
  if params.len <= 0:
    usage()
  
  for param in params:
    if param.startsWith "-o:":
      dir = param.split(":")[1]
    elif param != "":
      dburl = param

  if dir == "":
    dir = "."
    
  genSchemaFile(dir, dburl)
  
else:
  echo "not declared"
  usage()
