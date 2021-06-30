MERGE (v:Votazione {uri: $uri})
MERGE (p:Persona {fullname: $nome + ' ' + $cognome})
MERGE (voto:Voto {name: $espressione + ' ' + $uri + ' ' + $nome + ' ' + $cognome})
SET voto.fullname = $nome + ' ' + $cognome, voto.uri = $uri, voto.espressione= $espressione
//WITH voto,p,v
MERGE (p)-[:HA_VOTATO]->(voto)
MERGE (voto)-[:RELATIVO_ALLA_VOTAZIONE]->(v)
WITH voto
CALL apoc.do.when(
 $fazione <> '',
  'CALL apoc.create.addLabels(voto, [espressione, fazione])
   YIELD node
   RETURN node
  '
  ,
  'CALL apoc.create.addLabels(voto, [espressione])
   YIELD node
   RETURN node
   ',
  {voto:voto,
   fazione: $fazione,
   espressione:$espressione
  })
yield value
RETURN value