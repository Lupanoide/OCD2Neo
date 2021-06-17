from urllib.error import HTTPError
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
        self.seconds_timeout_sparql_httperror = 5

    def read_query_file(self, query_file):
        with open(query_file, "r") as f:
            return f.read()

    def get_neo_client(self, url):
        driver = GraphDatabase.driver(encrypted=False, uri=f"{url}", auth=("neo4j", "senato"))
        return driver

    def ingest_into_neo4j(self, query, **kwargs):
        with self.neo4j_client.session() as session:
            session.run(query, **kwargs)

    def get_result_sparql_query(self, sparql_query):
        self.sparql.setQuery(sparql_query)
        self.sparql.setReturnFormat(JSON)
        try:
            results = self.sparql.query().convert()
            output = None
            if results["results"]["bindings"]:
                output = results["results"]["bindings"]
            return output
        except HTTPError:
            print(f'Richiesta rigettata. Nuovo tentativo tra {self.seconds_timeout_sparql_httperror} secondi')
            time.sleep(self.seconds_timeout_sparql_httperror)
            self.get_result_sparql_query(sparql_query)
        except Exception as e:
            raise e

    def get_mese_anno_list(self):
        mesi = ["12","11","10","09","08","07","06","05","04","03","02","01"]
        anni = ["2021","2020","2019","2018"]
        resultL = []
        for anno in anni:
            if anno == "2021":
                mesi = mesi[6:]
            elif anno == "2018":
                mesi = mesi[:-2]
            for mese in mesi:
                resultL.append("^"+anno+mese)
        return resultL

    def get_anagrafica(self):
        lista_deputati = []
        query_anagrafica_sparql = self.read_query_file(os.path.join( os.path.dirname(__file__),'query_anagrafica.sparql'))
        query_anagrafica_cypher = self.read_query_file(os.path.join( os.path.dirname(__file__),'query_anagrafica.cypher'))
        query_sparql_result = self.get_result_sparql_query(query_anagrafica_sparql)

        print("INIZIO CARICAMENTO ANAGRAFICA")
        for result in query_sparql_result:
            self.ingest_into_neo4j(query_anagrafica_cypher, persona = result['persona']['value'],luogoNascita=result['luogoNascita']['value'],
                                                 collegio = result['collegio']['value'], lista=result.get('sigla',{}).get('value'), commissione=result.get('commissione',{}).get('value'),
                                                 numeroMandati = result['numeroMandati']['value'], genere= result['genere']['value'],info = result.get('info',{}).get('value'),
                                                 nomeGruppo = result['nomeGruppo']['value'], nome = result['nome']['value'], cognome = result['cognome']['value'],
                                                 dataNascita = result['dataNascita']['value'], inizioMandato = result['inizioMandato']['value'],
                                                 fineMandato = result.get('fineMandato',{}).get('value') )
            if not {"cognome": result['cognome']['value'],"nome":result['nome']['value']} in lista_deputati:
                lista_deputati.append({"cognome": result['cognome']['value'],"nome":result['nome']['value']})
        print("FINE CARICAMENTO ANAGRAFICA")
        return lista_deputati

    def get_atti_per_deputato(self, deputatiL, annoL):
        query_atti_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_atti.sparql'))
        query_atti_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_atti.cypher'))
        for deputato in deputatiL:
            for anno in annoL:
                param_query = query_atti_sparql.format(cognome=deputato['cognome'],nome=deputato['nome'],anno=anno)
                print(f"INIZIO CARICAMENTO ATTI PER DEPUTATO {deputato['nome']} {deputato['cognome']} per l'anno {anno[1:]}")
                query_sparql_result = self.get_result_sparql_query(param_query)

                if query_sparql_result:
                    for result in query_sparql_result:
                        self.ingest_into_neo4j(query_atti_cypher, nome = result['nome']['value'], cognome = result['cognome']['value'], atto = result['atto']['value'],
                                                         titolo = result['titolo']['value'].replace("'","\\'"), numeroAtto = result['numeroAtto']['value'], tipo = result['tipo']['value'],
                                                         tipoRuolo = result['tipoRuolo']['value'], date = result['date']['value'] )
                print(f"FINE CARICAMENTO ATTI PER DEPUTATO {deputato['nome']} {deputato['cognome']} per l'anno {anno[1:]}")

    def apply_constraints(self):
        query_constraints_cypher = self.read_query_file(os.path.join( os.path.dirname(__file__),'create_constraints.cypher'))
        for line in query_constraints_cypher.split("\n"):
            print(line)
            self.ingest_into_neo4j(line)
        print("FINE CARICAMENTO CONSTRAINTS")

    def get_votazioni_per_deputato(self, espressioneL, anno_mese_giornoL, deputatiL):
        query_votazione_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_voto_per_deputato.sparql'))
        query_votazione_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_voto_per_deputato.cypher'))
        for deputato in deputatiL:
            for espressione in espressioneL:
                for mese in anno_mese_giornoL:
                    param_query = query_votazione_sparql.format(nome = deputato['nome'], cognome= deputato['cognome'], giorno = mese, espressione= espressione)
                    print(f"INSERIMENTO VOTAZIONE {espressione} DELL'ONOREVOLE {deputato['nome'] + ' ' + deputato['cognome']} per il mese {mese[1:]}, ")
                    query_sparql_result = self.get_result_sparql_query(param_query)
                    if query_sparql_result:
                        for result in query_sparql_result:
                            self.ingest_into_neo4j(query_votazione_cypher, uri = result['votazione']['value'],nome = result['nome']['value'],
                                                   cognome = result['cognome']['value'], espressione= result['espressione']['value'])

    def get_votazioni(self, anno_mese_giornoL):
        query_votazione_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_votazioni.sparql"))
        query_votazione_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_votazioni.cypher"))
        for giorno in anno_mese_giornoL:
            param_query = query_votazione_sparql.format(giorno=giorno)
            query_sparql_result = self.get_result_sparql_query(param_query)
            if query_sparql_result:
                for result in query_sparql_result:
                    print(f"INGESTIONE VOTAZIONI DEL giorno {giorno[1:]}")
                    self.ingest_into_neo4j(query_votazione_cypher, uri = result['votazione']['value'], denominazione = result['denominazione']['value'],
                                           numeroVotazione = result['numeroVotazione']['value'], descrizione = result['descrizione']['value'],
                                           votanti = result['votanti']['value'],favorevoli = result['favorevoli']['value'], contrari = result['contrari']['value'],
                                           astenuti = result['astenuti']['value'], maggioranza = result['maggioranza']['value'], presenti = result['presenti']['value'],
                                           tipoVotazione = result['tipoVotazione']['value'],data=result['data']['value'],richiestaFiducia = result['richiestaFiducia']['value'],
                                           approvato = result['approvato']['value'], esitoVotazione = result['esitoVotazione']['value'], votazioneFinale = result['votazioneFinale']['value'],
                                           votazioneSegreta = result['votazioneSegreta']['value'], atto= result.get('atto',{}).get('value'), attoCamera = result.get('attoCamera',{}).get('value'))
                    print(f" VOTAZIONE {result['numeroVotazione']['value']} del giorno {giorno[1:]} CARICATA")

    def get_ogni_atto(self):
        query_ogni_atto_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_ogni_atto.sparql"))
        query_ogni_atto_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_ogni_atto.cypher"))
        query_sparql_result = self.get_result_sparql_query(query_ogni_atto_sparql)
        if query_sparql_result:
            print("INIZIO CARICAMENTO OGNI ATTO")
            for result in query_sparql_result:
                self.ingest_into_neo4j(query_ogni_atto_cypher, uri = result['atto']['value'], titolo= result['titolo']['value'],
                                       numero= result['numero']['value'], fase= result['fase']['value'],
                                       iniziativa= result['iniziativa']['value'], presentazione = result['presentazione']['value'],
                                       dataIter = result['dataIter']['value'], approvato= result.get('approvato',{}).get('value'),
                                       votazioneFinale=result.get('votazioneFinale', {}).get('value'),
                                       dataApprovazione= result.get('dataApprovazione',{}).get('value')
                )
            print("FINE CARICAMENTO OGNI ATTO")

    def run(self):
        espressioneL = ["Favorevole","Contrario", "Astensione", "Non ha votato"]
        tmp_list = pd.date_range('2018-03-23','2021-06-25',freq='24H').strftime("%Y%m%d").tolist()
        anno_mese_giornoL = ["^" + elem for elem in tmp_list][::-1]
        annoL = ["^2018","^2019","^2020","^2021"]
        self.apply_constraints()
        deputatiL = self.get_anagrafica()
        #self.get_atti_per_deputato(deputatiL, annoL)
        #self.get_ogni_atto()
        #self.get_votazioni(anno_mese_giornoL)
        self.get_votazioni_per_deputato(espressioneL, self.get_mese_anno_list(), deputatiL)



if __name__ == '__main__':
    oo = ImportCamera()
    oo.run()