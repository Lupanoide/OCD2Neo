MERGE (p:Persona {fullname: $nome + ' ' + $cognome })
MERGE (a:Atto {uri: $atto})
SET a.titolo = $titolo , a.numeroAtto = $numeroAtto, a.tipo = $tipo
MERGE (p)-[r:PRESENTA]->(a)
SET r.ruolo = $tipoRuolo
WITH a, [toInteger(substring($date, 0, 4)), toInteger(substring($date, 4,2)), toInteger(substring($date, 6,2))] AS dateComponents
SET a.data = date({year: dateComponents[0], month: dateComponents[1], day: dateComponents[2]})
WITH a
CALL apoc.do.when(
 $fazione <> '',
  'CALL apoc.create.addLabels(a, [fazione])
   YIELD node
   RETURN node
  '
  ,
  '',
  {a:a,
   fazione: $fazione
  })
yield value
RETURN value