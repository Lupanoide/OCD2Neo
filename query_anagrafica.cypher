MERGE (p:Persona {{uri: "{persona}" }})
SET p.nome = "{nome}", p.cognome = "{cognome}", p.numeroMandati = toInteger("{numeroMandati}"), p.genere = "{genere}", p.nomeGruppo = "{nomeGruppo}", p.fullname = "{nome}" + " " + "{cognome}" , p.inizioMandato = date({{year: toInteger(substring("{inizioMandato}", 0, 4)), month: toInteger(substring("{inizioMandato}", 4, 2)), day: toInteger(substring("{inizioMandato}", 6, 2))}})
MERGE (luogo:Luogo {{loc: "{luogoNascita}" }})
MERGE (c:Collegio {{ name: "{collegio}" }})

MERGE (p)-[:NATO_A]->(luogo)
MERGE (p)-[:ELETTO_NELLA_SEZIONE]->(c)
FOREACH (
lista in CASE WHEN "{lista}" = "" THEN [] ELSE ["{lista}"] END |
MERGE (l:Lista {{ name: "{lista}" }})
MERGE (p)-[:ISCRITTO_AL_PARTITO]->(l)
)
FOREACH (
info in CASE WHEN "{info}" = "" THEN [] ELSE "{info}" END |
SET p.info = split("{info}", ";")
)

FOREACH (
lista in CASE WHEN "{lista}" = "" THEN [] ELSE "{lista}" END |
MERGE (l:Lista {{name: "{lista}" }})
MERGE (p)-[:ISCRITTO_AL_PARTITO]->(l)
)
FOREACH (
commissione in CASE WHEN "{commissione}" = "" THEN [] ELSE ["{commissione}"] END |
MERGE (com:Commissione {{name: "{commissione}" }})
MERGE (p)-[:PARTECIPA_A_COMMISSIONE]->(com)
)
FOREACH (
fineMandato in CASE WHEN "{fineMandato}" = "" THEN [] ELSE ["{fineMandato}"] END |
SET p.fineMandato = date({{year: toInteger(substring("{fineMandato}", 0, 4)), month: toInteger(substring("{fineMandato}", 4, 2)), day: toInteger(substring("{fineMandato}", 6, 2))}})
)
WITH p, [toInteger(substring("{dataNascita}", 0, 4)), toInteger(substring("{dataNascita}", 4,2)), toInteger(substring("{dataNascita}", 6,2))] AS dateComponents
SET p.dataNascita = date({{year: dateComponents[0], month: dateComponents[1], day: dateComponents[2]}})