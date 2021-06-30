MATCH (n:Persona)-[r:ISCRITTO_AL_PARTITO]->(p:Lista) WHERE r.end_date = date("0000-01-01") REMOVE r.end_date;
MATCH (a:Atto) WHERE a.dataApprovazione = date("0000-01-01") REMOVE a.dataApprovazione;
CALL apoc.periodic.iterate('MATCH (v:Votazione) WHERE v.richiestaFiducia = true
RETURN id(v) as id_v',
    'MATCH (v) WHERE id(v) =id_v
    SET v:QuestioneDiFiducia',
    {batchSize: 1000, parallel: false});
CALL apoc.periodic.iterate('MATCH (v:Votazione) WHERE v.approvato = true
RETURN id(v) as id_v',
    'MATCH (v) WHERE id(v) =id_v
    SET v:Approvato',
    {batchSize: 1000, parallel: false});
CALL apoc.periodic.iterate("MATCH (a:Atto) WHERE a.tipo in ['MOZIONE','ODG - ORDINE DEL GIORNO IN ASSEMBLEA', 'ODG - ORDINE DEL GIORNO IN COMMISSIONE', 'RISOLUZIONE CONCLUSIVA', 'RISOLUZIONE IN COMMISSIONE','RISOLUZIONE IN ASSEMBLEA']
RETURN id(a) as id_a",
    'MATCH (a) WHERE id(a) =id_a
    SET a:AttodiIndirizzo',
    {batchSize: 1000, parallel: false});
CALL apoc.periodic.iterate("MATCH (a:Atto) WHERE a.tipo in ['INTERROGAZIONE A RISPOSTA SCRITTA','INTERROGAZIONE A RISPOSTA ORALE', 'INTERROGAZIONE A RISPOSTA IN COMMISSIONE', 'INTERPELLANZA']
RETURN id(a) as id_a",
    'MATCH (a) WHERE id(a) =id_a
    SET a:AttodiControllo',
    {batchSize: 1000, parallel: false});
