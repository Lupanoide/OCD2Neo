MERGE (v:Votazione {uri: $uri})
SET v.numeroVotazione = $numeroVotazione, v.denominazione = $denominazione, v.descrizione = $descrizione,
v.votanti = toInteger($votanti), v.favorevoli = toInteger($favorevoli), v.contrari = toInteger($contrari), v.astenuti=toInteger($astenuti),
v.maggioranza = toInteger($maggioranza), v.presenti = toInteger($presenti), v.tipoVotazione = $tipoVotazione
WITH v, [toInteger(substring($data, 0, 4)), toInteger(substring($data, 4,2)), toInteger(substring($data, 6,2))] AS dateComponents
SET v.data = date({year: dateComponents[0], month: dateComponents[1], day: dateComponents[2]})
WITH
CASE $richiestaFiducia
WHEN '0' THEN false
WHEN '1' THEN true
END AS richiestaFiducia, v
SET v.richiestaFiducia = richiestaFiducia
WITH
CASE $approvato
WHEN '0' THEN false
WHEN '1' THEN true
END AS approvato, v
SET v.approvato = approvato
WITH
CASE $esitoVotazione
WHEN '0' THEN false
WHEN '1' THEN true
END AS esitoVotazione, v
SET v.esitoVotazione = esitoVotazione
WITH
CASE $votazioneFinale
WHEN '0' THEN false
WHEN '1' THEN true
END AS votazioneFinale, v
SET v.votazioneFinale = votazioneFinale
WITH
CASE $votazioneSegreta
WHEN '0' THEN false
WHEN '1' THEN true
END AS votazioneSegreta, v
SET v.votazioneSegreta = votazioneSegreta
FOREACH (
atto IN CASE WHEN $atto = '' THEN []
  ELSE [$atto]
  END |
  MERGE (a:Atto {uri: $atto})
  MERGE (v)-[:SI_RIFERISCE]->(a)
)
FOREACH (
attoCamera IN CASE WHEN $attoCamera = {} THEN []
  ELSE [$attoCamera]
  END |
  MERGE (a:Atto {uri: $attoCamera})
  MERGE (v)-[:SI_RIFERISCE]->(a)
)