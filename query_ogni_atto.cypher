MERGE (a:Atto {uri: $uri})
SET a.titolo = $titolo, a.numeroAtto = $numero, a.fase = $fase
WITH a
CALL apoc.create.addLabels( a, [ $iniziativa ] )
YIELD node

WITH node, [toInteger(substring($presentazione, 0, 4)), toInteger(substring($presentazione, 4,2)), toInteger(substring($presentazione, 6,2))] AS dateComponents,
[toInteger(substring($dataIter, 0, 4)), toInteger(substring($dataIter, 4,2)), toInteger(substring($dataIter, 6,2))] AS dateIter
SET node.data = date({year: dateComponents[0], month: dateComponents[1], day: dateComponents[2]})
SET node.dataIterFase = date({year: dateIter[0], month: dateIter[1], day: dateIter[2]})
WITH node
CALL apoc.do.when(
 $dataApprovazione <> '',
  'SET node.dataApprovazione = date({year: toInteger(substring(dataApprovazione, 0, 4)), month: toInteger(substring(dataApprovazione, 4,2)), day: toInteger(substring(dataApprovazione, 6,2))})

  WITH
  CASE votazioneFinale
  WHEN "0" THEN false
  WHEN "1" THEN true
  END AS votazioneFinale, node, approvato
  SET node.votazioneFinale = votazioneFinale

  WITH
  CASE approvato
  WHEN "0" THEN false
  WHEN "1" THEN true
  END AS approvato, node
  SET node.approvato = approvato

'
  ,
  '',
  {node:node,
   votazioneFinale: $votazioneFinale,
   dataApprovazione:$dataApprovazione,
   approvato:$approvato
  })
yield value
RETURN value


