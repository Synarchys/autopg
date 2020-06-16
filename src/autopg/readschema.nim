import db_postgres, json, sequtils, sugar, strutils, strformat
import webschema
import pgschemautils / [ pgschema, pgcquery ] 

proc toColumn(c: PG_column): Column =
  result = Column()
  result.name = c.column_name
  result.position = c.ordinal_position
  result.length = c.character_maximum_length
  result.data_type = c.data_type


  
proc readTables*(pg: DBConn): seq[Table] =
  result = @[]
  let pg_tables = pg.get_pgtables()
  let pg_cols = pg.get_pgcolumns()
  let col_comms = pg.get_column_comments()
  let tab_comms = pg.get_table_comments()
  for pt in pg_tables:
    var t = Table()
    t.columns = @[]
    t.name = pt.table_name
    t.comment = tab_comms.filter( tc => tc.table_name == t.name)[0].comment
    for c in pg_cols:
      if c.table_name == t.name:
        var col = c.toColumn()
        col.comment = col_comms.filter(cc => cc.table_name == t.name and
                                       cc.column_name == col.name)[0].comment
        t.columns.add(col)
    result.add(t)
  #let rels = pg.getAllRows(sql"select * from invrep_aux.relations")
#  let rels = pg.get_pgconstraints()
#  var relations:seq[Relation] = @[]
#  for r in rels:
    # create all relations
#    var rel = Relation()
#    for t in result:
#      if t.name ==  r[1]: 
#        rel.fromTable = t
#        t.relations.add(rel)
#      if t.name ==  r[3]:
#        rel.toTable = t
#        t.relations.add(rel)
#      for c in t.columns:
#        if c.name == r[2]:
#           rel.fromColumn = c
#        if c.name == r[4]:
#          rel.toColumn = c

proc toGLSchema*(schema: seq[Table]): JsonNode =
  var gtypes = %*{}
  for t in schema:
    var props = %*{}
    for c in t.columns:
      props[c.name] = %*{"comment": c.comment, "type":
        c.data_type, "order": c.position}
    for r in t.relations:
      if r.fromTable == t:
        props[r.toTable.name] = %*{ "comment" : r.toTable.comment,
                                     "type": r.toTable.name,
                                     "foreign_key": r.toColumn.name,
                                     "local_key": r.fromColumn.name
        }
      if r.toTable == t:
        props[r.fromTable.name] = %*{ "comment" : r.fromTable.comment,
                                     "type": [r.fromTable.name],
                                     "foreign_key": r.fromColumn.name
        }
    gtypes[t.name] = %*{
      "comment": t.comment,
      "properties": props
      }
  result = %*{"schema": {"name": "public", "types": gtypes }}
        
proc toJson*(schema: seq[Table]): JsonNode =
  result = newJArray()
  for t in schema:
    let cols = %t.columns
    let rels = t.relations.map(r => %*{"name":r.name,
                           "fromTable": r.fromTable.name,
                           "fromColumn": r.fromColumn.name,
                           "toTable": r.toTable.name,
                           "toColumn": r.toColumn.name})
    result.add(%*{"name": t.name,
              "comment": t.comment,
              "columns": cols,
              "relations": rels})

