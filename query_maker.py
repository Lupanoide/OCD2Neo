import sparql

q = ("""
  SELECT DISTINCT ?persona ?cognome ?nome ?info
?dataNascita ?luogoNascita ?genere
?collegio ?lista ?nomeGruppo  COUNT(DISTINCT ?madatoCamera) as ?numeroMandati ?aggiornamento
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.

## deputato
?d a ocd:deputato;
ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>;
ocd:rif_mandatoCamera ?mandato.
OPTIONAL{?d ocd:aderisce ?aderisce} 
OPTIONAL{?d dc:description ?info}

##anagrafica
?d foaf:surname ?cognome; foaf:gender ?genere;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
?luogoNascitaUri dc:title ?luogoNascita.
}

##aggiornamento del sistema
OPTIONAL{?d <http://lod.xdams.org/ontologies/ods/modified> ?aggiornamento.}

## mandato
?mandato ocd:rif_elezione ?elezione. 
MINUS{?mandato ocd:endDate ?fineMandato.}
 
## totale mandati
?persona ocd:rif_mandatoCamera ?madatoCamera.

## elezione
OPTIONAL {
?elezione dc:coverage ?collegio
}
OPTIONAL {
?elezione ocd:lista ?lista
}

## adesione a gruppo
OPTIONAL {?aderisce ocd:rif_gruppoParlamentare ?gruppo}
OPTIONAL {?aderisce rdfs:label ?nomeGruppo}
MINUS{?aderisce ocd:endDate ?fineAdesione}
 
}
""")
lista_senatori = []
result = sparql.query('http://dati.camera.it/sparql', q)
for row in result:
    lista_senatori.append( row[1] )


for cognome in lista_senatori:
    i= 'i'
    q = (f"#### tutte le espressioni di voto di un deputato nella legislatura
#### (es. deputato - SPERANZA - XVIII legislatura - <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>) con filtro sulla data
#### (data specifica, tutto l'anno o mese ed anno es. mese di settembre 2018 - FILTER&#40;REGEX&#40;?data,'^201809','i')) ) con filtro sulla espressione di voto
#### (favorevole,contrario, Ha votato, Non ha votato, Astensione es. favorevole - FILTER&#40;REGEX&#40;espressione,'Favorevole','i')) nessun filtro:<br /> ## FILTER&#40;REGEX&#40;?espressione,'','i')) )

select distinct ?cognome ?nome ?votazione ?data ?descrizione ?numeroVotazione
?espressione ?esitoVotazione
?infoAssenza where {

## seleziono deputato nella legislatura
?deputato foaf:surname ?cognome; foaf:firstName ?nome; ocd:rif_leg
<http://dati.camera.it/ocd/legislatura.rdf/repubblica_18> .
FILTER(REGEX(?cognome,{cognome},{i}))

## voti espressi dal deputato
?voto a ocd:voto;
   ocd:rif_votazione ?votazione;
   dc:type ?espressione;
   ocd:rif_deputato ?deputato.
   	## FILTER(REGEX(?espressione,'Favorevole','i'))
 OPTIONAL{?voto dc:description ?infoAssenza}


## informazione sulla relativa votazione
?votazione a ocd:votazione;
   ocd:approvato ?esitoVotazione;
   dc:description ?descrizione;
   dc:identifier ?numeroVotazione;
## filtro sulla data si può scegliere anno/mese ('^201809') o anno ('^2018') o anche una data ('20180904')
	dc:date ?data.
  ## FILTER(REGEX(?data,'^201809','i'))
## FILTER(REGEX(?infoAssenza,'In missione','i'))

} ORDER BY ?numeroVotazione LIMIT 10")