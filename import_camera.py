from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import time
import pandas as pd


class ImportCamera:
    def __init__(self):
        neo_url = r"bolt://localhost:7687"
        self.neo4j_client = self.get_neo_client(neo_url)
        self.sparql = SPARQLWrapper(r'http://dati.camera.it/sparql')
        self.sparql.setTimeout(timeout=(180))

    def read_query_file(self, query_file):
        with open(query_file, "r") as f:
            return f.read()

    def get_neo_client(self, url):
        driver = GraphDatabase.driver(encrypted=False, uri=f"{url}", auth=("neo4j", "senato"))
        return driver

    def ingest_into_neo4j(self, query, **kwargs):
        with self.neo4j_client.session() as session:
            session.run(query,**kwargs)

    def get_anagrafica(self):
        lista_deputati = []
        query_anagrafica_sparql = self.read_query_file(os.path.join( os.path.dirname(__file__),'query_anagrafica.sparql'))
        query_anagrafica_cypher = self.read_query_file(os.path.join( os.path.dirname(__file__),'query_anagrafica.cypher'))
        self.sparql.setQuery(query_anagrafica_sparql)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        print("INIZIO CARICAMENTO ANAGRAFICA")
        for result in results["results"]["bindings"]:
            self.ingest_into_neo4j(query_anagrafica_cypher, persona = result['persona']['value'],luogoNascita=result['luogoNascita']['value'],
                                                 collegio = result['collegio']['value'], lista=result.get('sigla',{}).get('value',''), commissione=result.get('commissione',{}).get('value',''),
                                                 numeroMandati = result['numeroMandati']['value'], genere= result['genere']['value'],info = result.get('info',{}).get('value',''),
                                                 nomeGruppo = result['nomeGruppo']['value'], nome = result['nome']['value'], cognome = result['cognome']['value'],
                                                 dataNascita = result['dataNascita']['value'], inizioMandato = result['inizioMandato']['value'],
                                                 fineMandato = result.get('fineMandato',{}).get('value','') )
            lista_deputati.append({"cognome": result['cognome']['value'],"nome":result['nome']['value']})
        print("FINE CARICAMENTO ANAGRAFICA")
        return lista_deputati

    def get_atti_per_deputato(self, deputatiL, annoL):
        query_atti_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_atti.sparql'))
        query_atti_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_atti.cypher'))
        #get_index = deputatiL.index({"cognome":"LOTTI","nome":"LUCA"})
        for deputato in deputatiL:
        #for deputato in deputatiL[get_index:]:
            for anno in annoL:
                param_query = query_atti_sparql.format(cognome=deputato['cognome'],nome=deputato['nome'],anno=anno)
                print(param_query)
                self.sparql.setQuery(param_query)
                self.sparql.setReturnFormat(JSON)
                results = self.sparql.query().convert()

                print(f"INIZIO CARICAMENTO ATTI PER DEPUTATO {deputato['nome']} {deputato['cognome']} per l'anno {anno[1:]}")
                if results["results"]["bindings"]:
                    for result in results["results"]["bindings"]:
                        self.ingest_into_neo4j(query_atti_cypher, nome = result['nome']['value'], cognome = result['cognome']['value'], atto = result['atto']['value'],
                                                         titolo = result['titolo']['value'].replace("'","\\'"), numeroAtto = result['numeroAtto']['value'], tipo = result['tipo']['value'],
                                                         tipoRuolo = result['tipoRuolo']['value'], date = result['date']['value'] )
                print(f"FINE CARICAMENTO ATTI PER DEPUTATO {deputato['nome']} {deputato['cognome']} per l'anno {anno[1:]}")
                time.sleep(3)



    def apply_constraints(self):
        query_constraints_cypher = self.read_query_file(os.path.join( os.path.dirname(__file__),'create_constraints.cypher'))
        for line in query_constraints_cypher.split("\n"):
            print(line)
            self.ingest_into_neo4j(line)
        print("FINE CARICAMENTO CONSTRAINTS")

    def get_votazioni_per_deputato(self, espressioneL, anno_mese_giornoL, deputatiL):
        query_votazione_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_voto_per_deputato.sparql'))
        for deputato in deputatiL:
            for espressione in espressioneL:
                for giorno in anno_mese_giornoL:
                    param_query = query_votazione_sparql.format(nome = deputato['nome'], cognome= deputato['cognome'], giorno = giorno, espressione= espressione)
                    print(param_query)
                    self.sparql.setQuery(param_query)
                    self.sparql.setReturnFormat(JSON)
                    results = self.sparql.query().convert()
                    if results["results"]["bindings"]:
                        for result in results["results"]["bindings"]:
                            print(result)
                            break

    def get_votazioni(self, anno_mese_giornoL):
        query_votazione_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_votazioni.sparql"))
        query_votazione_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_votazioni.cypher"))
        for giorno in anno_mese_giornoL:
            param_query = query_votazione_sparql.format(giorno=giorno)
            print(param_query)
            self.sparql.setQuery(param_query)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()
            if results["results"]["bindings"]:
                for result in results["results"]["bindings"]:
                    print(result)
                    print(result.get('attoCamera.value', ''))
                    print(result.get('atto.value', ''))
                    self.ingest_into_neo4j(query_votazione_cypher, uri = result['votazione']['value'], denominazione = result['denominazione']['value'],
                                           numeroVotazione = result['numeroVotazione']['value'], descrizione = result['descrizione']['value'],
                                           votanti = result['votanti']['value'],favorevoli = result['favorevoli']['value'], contrari = result['contrari']['value'],
                                           astenuti = result['astenuti']['value'], maggioranza = result['maggioranza']['value'], presenti = result['presenti']['value'],
                                           tipoVotazione = result['tipoVotazione']['value'],data=result['data']['value'],richiestaFiducia = result['richiestaFiducia']['value'],
                                           approvato = result['approvato']['value'], esitoVotazione = result['esitoVotazione']['value'], votazioneFinale = result['votazioneFinale']['value'],
                                           votazioneSegreta = result['votazioneSegreta']['value'], atto= result.get('atto',{}).get('value',''), attoCamera = result.get('attoCamera',{}).get('value',''))
                    print(f"UNA VOTAZIONE DEL giorno {giorno} CARICATA")
                    print(f"atto {result.get('atto.value','')}")
                    print(f"attoCamera {result.get('attoCamera.value', '')}")


    def run(self):
        #espressioneL = ["Favorevole","Contrario", "Astensione"]
        tmp_list = pd.date_range('2018-03-23','2021-02-24',freq='24H').strftime("%Y%m%d").tolist()
        anno_mese_giornoL = ["^" + elem for elem in tmp_list][::-1]
        annoL = ["^2018","^2019","^2020","^2021"]
        self.apply_constraints()
        deputatiL = self.get_anagrafica()
        #self.get_atti_per_deputato(deputatiL, annoL)
        self.get_votazioni(anno_mese_giornoL)
        #self.get_votazioni_per_deputato(espressioneL, anno_mese_giornoL, deputatiL)





if __name__ == '__main__':
    oo = ImportCamera()
    oo.run()
    del oo