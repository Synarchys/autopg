type
  Constraint* = ref object
    name*: string
    table*: Table
    column*: Column
    statement*: string

  Relation* = ref object
    name*: string
    fromTable*: Table
    fromColumn*: Column
    toTable*: Table
    toColumn*: Column
    
  Column* =  ref object
    name*: string
    comment*: string
    position*: int
    length*: int
    data_type*: string
    #constraints*: seq[Constraint]
    
  Table* = ref object
    name*: string
    comment*: string
    columns*: seq[Column] 
    relations*: seq[Relation]

import strformat

proc getTable*(tables: seq[Table], name: string): Table = 
  #echo fmt"Get table: {name}"
  for t in tables:
    if t.name == name:
      #echo fmt"Found! {name}"
      return t 
    # else:
    #   echo fmt"table {name} not found"

proc getColumn*(t: Table, name: string): Column =
  for c in t.columns:
    if c.name == name:
      return c 
    # else:
    #   echo fmt"column {name} not found"
      
proc getRelation*(t: Table, name: string): Relation =
  for r in t.relations:
    if r.name == name:
      return r 
    # else:
    #   echo fmt"relation {name} not found"

#[ 
    TODO:
      - implement constraints
      - get constraints by column
 ]#
