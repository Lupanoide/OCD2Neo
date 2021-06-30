MERGE (p:Persona {uri: $persona })
SET p:Deputato, p.nome = $nome, p.cognome = $cognome, p.numeroMandati = toInteger($numeroMandati), p.genere = $genere, p.fullname = $nome + " " + $cognome
MERGE (luogo:Luogo {loc: $luogoNascita })
MERGE (c:Collegio { name: $collegio })

MERGE (p)-[:NATO_A]->(luogo)
MERGE (p)-[:ELETTO_NELLA_SEZIONE]->(c)

MERGE (l:Lista {name: $lista })
MERGE (p)-[r:ISCRITTO_AL_PARTITO]->(l)

FOREACH (
info in CASE WHEN $info = '' THEN [] ELSE $info END |
SET p.info = split($info, ";")
)

FOREACH (
commissione in CASE WHEN $commissione = '' THEN [] ELSE [$commissione] END |
MERGE (com:Commissione {name: $commissione })
MERGE (p)-[:PARTECIPA_A_COMMISSIONE]->(com)
)
WITH p, [toInteger(substring($dataNascita, 0, 4)), toInteger(substring($dataNascita, 4,2)), toInteger(substring($dataNascita, 6,2))] AS dateComponents
SET p.dataNascita = date({year: dateComponents[0], month: dateComponents[1], day: dateComponents[2]})