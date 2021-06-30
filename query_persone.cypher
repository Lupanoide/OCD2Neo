MERGE (p:Persona {uri: $uri })
SET p.nome = $nome, p.cognome = $cognome, p.genere = $genere, p.fullname = $nome + " " + $cognome
MERGE (luogo:Luogo {loc: $luogoNascita })
MERGE (p)-[:NATO_A]->(luogo)
MERGE (l:Lista { name: $lista })
MERGE (p)-[r:ISCRITTO_AL_PARTITO]->(l)
SET r.start_date = date({year: toInteger(substring( $inizioMandato, 0, 4)), month: toInteger(substring($inizioMandato, 4, 2)), day: toInteger(substring($inizioMandato, 6, 2))})
FOREACH (
  is_ended in CASE WHEN $end_date <> '' THEN [$end_date] ELSE [] END |
  SET r.end_date = date({year: toInteger(substring( $end_date, 0, 4)), month: toInteger(substring($end_date, 4, 2)), day: toInteger(substring($end_date, 6, 2))}),
  r.motivoTermine = $motivoTermine

)
WITH p, [toInteger(substring($dataNascita, 0, 4)), toInteger(substring($dataNascita, 4,2)), toInteger(substring($dataNascita, 6,2))] AS dateComponents
SET p.dataNascita = date({year: dateComponents[0], month: dateComponents[1], day: dateComponents[2]})