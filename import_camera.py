from urllib.error import HTTPError, URLError
from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON
from SPARQLWrapper.SPARQLExceptions import EndPointNotFound
import os
import time
import pandas as pd
from datetime import datetime


class ImportCamera:
    def __init__(self):
        neo_url = r"bolt://localhost:7687"
        neo_auth = ("neo4j", "senato")
        sparql_endpoint = r'http://dati.camera.it/sparql'
        self.neo4j_client = self.__get_neo_client(neo_url, neo_auth)
        self.sparql = self.__get_sparql_endpoint_interface(sparql_endpoint)
        self.seconds_timeout_sparql_httperror = 5

    def __get_sparql_endpoint_interface(self, sparql_endpoint):
        sparql = SPARQLWrapper(sparql_endpoint)
        sparql.setTimeout(timeout=(180))
        sparql.setUseKeepAlive()
        return sparql

    def read_query_file(self, query_file):
        with open(query_file, "r") as f:
            return f.read()

    def __get_neo_client(self, url, auth):
        driver = GraphDatabase.driver(encrypted=False, uri=f"{url}", auth=auth)
        return driver

    def ingest_into_neo4j(self, query, **kwargs):
        try:
            with self.neo4j_client.session() as session:
                session.run(query, **kwargs)
        except Exception as e:
            raise e

    def get_result_sparql_query(self, sparql_query):
        self.sparql.setQuery(sparql_query)
        self.sparql.setReturnFormat(JSON)
        output = None
        try:
            results = self.sparql.query().convert()
            if results["results"]["bindings"]:
                output = results["results"]["bindings"]
        except (HTTPError, EndPointNotFound, URLError) as lost_connection:
            print(f'Richiesta verso SPARQL endpoint rigettata. Nuovo tentativo tra {self.seconds_timeout_sparql_httperror} secondi')
            time.sleep(self.seconds_timeout_sparql_httperror)
            self.get_result_sparql_query(sparql_query)
        except Exception as e:
            raise e
        else:
            return output

    def get_maggioranze_legislature(self):
        return {"Conte I": {'start_date': datetime(2018,5,31,0,0),'end_date': datetime(2019,9,4,0,0),'maggioranza': ['M5S','LEGA'], 'person': ['SILVIA BENEDETTI', 'MARIO ALEJANDRO BORGHESE', 'SALVATORE CAIATA', 'ANDREA CECCONI','FAUSTO GUILHERME LONGO','ANTONIO TASSO' ,'CATELLO VITIELLO', 'RENATE GEBHARD','ALBRECHT PLANGGER','EMANUELA ROSSINI', 'MANFRED SCHULLIAN']},
                "Conte II": {'start_date': datetime(2019,9,5,0,0), 'end_date': datetime(2021,1,13,00), 'maggioranza': ['LEU','PD', 'IV','M5S'], 'person':['RENATE GEBHARD','ALBRECHT PLANGGER','EMANUELA ROSSINI', 'MANFRED SCHULLIAN']},
                "Conte II bis": {'start_date': datetime(2021,1,14,00), 'end_date': datetime(2021,2,12,0,0), 'maggioranza': ['LEU', 'PD', 'M5S'], 'person':['MICHELA ROSTAN', 'ANTONIO LOMBARDO', 'MARA LAPIA', 'FABIO BERARDINI', 'CARLO UGO DE GIROLAMO' ,'MARCO RIZZONE', 'NICOLA ACUNZO', 'DANIELA CARDINALE', 'CARMELO LO MONTE', 'RENATA POLVERINI', 'RENATE GEBHARD','ALBRECHT PLANGGER','EMANUELA ROSSINI', 'MANFRED SCHULLIAN','MARIO ALEJANDRO BORGHESE','FAUSTO GUILHERME LONGO','ANTONIO TASSO','NICOLA ACUNZO','PIERA AIELLO','DANIELA CARDINALE','ALESSANDRA ERMELLINO','MARA LAPIA','CARMELO LO MONTE', 'BRUNO TABACCI','NADIA APRILE','SILVIA BENEDETTI','ROSALBA DE GIORGI','LORENZO FIORAMONTI','RAFFAELE TRANO']},
                "Draghi":{'start_date':datetime(2021,2,13,0,0), 'end_date': datetime.now(), 'maggioranza': ['M5S','LEGA','PD','FI','IV','CI','LEU'], 'person': ['MARIO ALEJANDRO BORGHESE','FAUSTO GUILHERME LONGO','ANTONIO TASSO', 'RENATE GEBHARD','ALBRECHT PLANGGER','EMANUELA ROSSINI', 'MANFRED SCHULLIAN','NICOLA ACUNZO','PIERA AIELLO','DANIELA CARDINALE','ALESSANDRA ERMELLINO','MARA LAPIA','CARMELO LO MONTE', 'BRUNO TABACCI','ALESSANDRO COLUCCI','MAURIZIO ENZO LUPI', 'EUGENIO SANGREGORIO','RENZO TONDO','NUNZIO ANGIOLA','ENRICO COSTA','RICCARDO MAGI', 'ANDREA CECCONI','LORENZO FIORAMONTI','ALESSANDRO FUSACCHIA','ANTONIO LOMBARDO','ROSSELLA MURONI','NADIA APRILE','ROSALBA DE GIORGI','MICHELA ROSTAN' ]}}

    def get_mese_anno_list(self):
        tmp_list = pd.date_range('2018-03-23', datetime.strftime(datetime.now(), "%Y-%m-%d"), freq='1m').strftime("%Y%m").tolist()
        resultL = ["^" + elem for elem in tmp_list][::-1]
        return resultL

    def get_anagrafica(self):
        query_anagrafica_sparql = self.read_query_file(os.path.join( os.path.dirname(__file__),'query_anagrafica.sparql'))
        query_anagrafica_cypher = self.read_query_file(os.path.join( os.path.dirname(__file__),'query_anagrafica.cypher'))
        query_sparql_result = self.get_result_sparql_query(query_anagrafica_sparql)

        print("INIZIO CARICAMENTO ANAGRAFICA")
        for result in query_sparql_result:
            self.ingest_into_neo4j(query_anagrafica_cypher, persona = result['persona']['value'],luogoNascita=result['luogoNascita']['value'],
                                                 collegio = result['collegio']['value'], lista=result['sigla']['value'], commissione=result.get('commissione',{}).get('value',''),
                                                 numeroMandati = result['numeroMandati']['value'], genere= result['genere']['value'],info = result.get('info',{}).get('value',''),
                                                 nome = result['nome']['value'], cognome = result['cognome']['value'],
                                                 dataNascita = result['dataNascita']['value'] )
        print("FINE CARICAMENTO ANAGRAFICA")

    def get_atti_per_deputato(self, deputatiDiz, annoL):
        query_atti_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_atti.sparql'))
        query_atti_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_atti.cypher'))
        #get_index = list(deputatiDiz.keys()).index("PAOLO GIULIODORI")
        for deputato in deputatiDiz.keys():
        #for deputato in list(deputatiDiz.keys())[get_index:]:
            for anno in annoL:
                param_query = query_atti_sparql.format(cognome=deputatiDiz[deputato]['cognome'],nome=deputatiDiz[deputato]['nome'],anno=anno)
                print(f"INIZIO CARICAMENTO ATTI PER DEPUTATO {deputatiDiz[deputato]['nome']} {deputatiDiz[deputato]['cognome']} per l'anno {anno[1:]}")
                query_sparql_result = self.get_result_sparql_query(param_query)

                if query_sparql_result:
                    for result in query_sparql_result:
                        fazione = self.get_fazione_politica(result['date']['value'].split('-')[0], deputatiDiz[deputato])
                        self.ingest_into_neo4j(query_atti_cypher, nome = result['nome']['value'], cognome = result['cognome']['value'], atto = result['atto']['value'],
                                                         titolo = result['titolo']['value'].replace("'","\\'"), numeroAtto = result['numeroAtto']['value'], tipo = result['tipo']['value'],
                                                         tipoRuolo = result['tipoRuolo']['value'], date = result['date']['value'], fazione=fazione )
                print(f"FINE CARICAMENTO ATTI PER DEPUTATO {deputatiDiz[deputato]['nome']} {deputatiDiz[deputato]['cognome']} per l'anno {anno[1:]}")

    def apply_constraints(self):
        query_constraints_cypher = self.read_query_file(os.path.join( os.path.dirname(__file__),'create_constraints.cypher'))
        for line in query_constraints_cypher.split("\n"):
            print(line)
            self.ingest_into_neo4j(line)
        print("FINE CARICAMENTO CONSTRAINTS")

    def get_fazione_politica(self, data, partiti_per_deputato_diz):
        data = datetime.strptime(data, '%Y%m%d')
        output = ''
        if data < datetime(2018,5,31,0,0):
            return 'precedenteAlPrimoGovernoConte'
        fullname = partiti_per_deputato_diz['nome'] + ' ' + partiti_per_deputato_diz['cognome']
        governi = self.get_maggioranze_legislature()
        for governo in governi.keys():
            if governi[governo]['start_date'] <= data <= governi[governo]['end_date']:
                if fullname in governi[governo]['person']:
                    output = "ProvenienteDaMaggioranza"
                    break
                else:
                    for item in partiti_per_deputato_diz['partiti']:
                        if item['start_date'] <= data <= item['end_date']:
                            if item['lista'] in governi[governo]['maggioranza']:
                                output = "ProvenienteDaMaggioranza"
                                break
                            else:
                                output = "ProvenienteDaOpposizione"
                                break
        return output


    def get_votazioni_per_deputato(self, espressioneL, anno_mese_giornoL, deputatiDiz):
        query_votazione_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_voto_per_deputato.sparql'))
        query_votazione_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_voto_per_deputato.cypher'))
        #get_index = list(deputatiDiz.keys()).index("DOMENICO GIANNETTA")
        for deputato in deputatiDiz.keys():
        #for deputato in list(deputatiDiz.keys())[get_index:]:
            for espressione in espressioneL:
                for giorno in anno_mese_giornoL:
                    param_query = query_votazione_sparql.format(nome = deputatiDiz[deputato]['nome'], cognome= deputatiDiz[deputato]['cognome'], giorno = giorno, espressione= espressione)
                    query_sparql_result = self.get_result_sparql_query(param_query)
                    if query_sparql_result:
                        for result in query_sparql_result:
                            fazione = self.get_fazione_politica(result['data']['value'], deputatiDiz[deputato])
                            self.ingest_into_neo4j(query_votazione_cypher, uri = result['votazione']['value'],nome = result['nome']['value'],
                                                   cognome = result['cognome']['value'], espressione= result['espressione']['value'], fazione=fazione)
                    print(f"INSERITI I VOTI {espressione} DELL'ONOREVOLE {deputatiDiz[deputato]['nome']} {deputatiDiz[deputato]['cognome']} per il mese {giorno[1:]}")

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
                                           votazioneSegreta = result['votazioneSegreta']['value'], atto= result.get('atto',{}).get('value',''), attoCamera = result.get('attoCamera',{}).get('value',''))
                    print(f" VOTAZIONE {result['numeroVotazione']['value']} del giorno {giorno[1:]} CARICATA")

    def get_ogni_atto(self, annoL):
        query_ogni_atto_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_ogni_atto.sparql"))
        query_ogni_atto_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_ogni_atto.cypher"))
        for anno in annoL:
            param_query = query_ogni_atto_sparql.format(anno=anno)
            query_sparql_result = self.get_result_sparql_query(param_query)
            if query_sparql_result:
                print(f"INIZIO CARICAMENTO OGNI ATTO PER L'ANNO {anno[1:]}")
                for result in query_sparql_result:
                    self.ingest_into_neo4j(query_ogni_atto_cypher, uri = result['atto']['value'], titolo= result['titolo']['value'],
                                           numero= result['numero']['value'], fase= result['fase']['value'],
                                           iniziativa= result['iniziativa']['value'], presentazione = result['presentazione']['value'],
                                           dataIter = result['dataIter']['value'], approvato= result.get('approvato',{}).get('value',''),
                                           votazioneFinale=result.get('votazioneFinale', {}).get('value',''),
                                           dataApprovazione= result.get('dataApprovazione',{}).get('value','')
                    )
                print(f"FINE CARICAMENTO OGNI ATTO PER L'ANNO {anno[1:]}")

    def get_persona(self):
        personaDiz = {}
        query_ogni_atto_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_persone.sparql"))
        query_ogni_atto_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), "query_persone.cypher"))
        query_sparql_result = self.get_result_sparql_query(query_ogni_atto_sparql)
        if query_sparql_result:
            for result in query_sparql_result:
                self.ingest_into_neo4j(query_ogni_atto_cypher, uri=result['persona']['value'], cognome=result['cognome']['value'], nome=result['nome']['value'],
                                           genere=result['genere']['value'], dataNascita=result['dataNascita']['value'], luogoNascita=result['luogoNascita']['value'],
                                           lista =result['sigla']['value'], inizioMandato=result['date']['value'], end_date=result.get('end_date',{}).get('value',''),
                                       motivoTermine=result.get('motivoTermine',{}).get('value',''))
                if result['nome']['value'] + ' ' + result['cognome']['value'] not in personaDiz:
                    personaDiz[result['nome']['value'] + ' ' + result['cognome']['value']] = {
                        'nome':result['nome']['value'], 'cognome':result['cognome']['value'], 'partiti' :[]}
                if 'end_date' not in result:
                    result['end_date'] = {'value' : datetime.strftime(datetime.now(), "%Y%m%d")}
                personaDiz[result['nome']['value'] + ' ' + result['cognome']['value']]['partiti'].append(
                    {'start_date': datetime.strptime(result['date']['value'], "%Y%m%d") , 'end_date' : datetime.strptime(result['end_date']['value'] , "%Y%m%d" ), 'lista': result['sigla']['value'],} )
        return personaDiz

    def clean_data(self):
        query_clean_data_cypher = self.read_query_file(
            os.path.join(os.path.dirname(__file__), 'clean_data.cypher'))
        for line in query_clean_data_cypher.split(";"):
            print(line)
            if line.strip():
                self.ingest_into_neo4j(line)
        print("FINE PULIZIA")

    def run(self):
        espressioneL = ["Favorevole","Contrario", "Astensione", "Non ha votato"]
        tmp_list = pd.date_range('2018-03-23',datetime.strftime(datetime.now(), "%Y-%m-%d"),freq='24H').strftime("%Y%m%d").tolist()
        anno_mese_giornoL = ["^" + elem for elem in tmp_list][::-1]
        annoL = ["^2018","^2019","^2020","^2021"]
        self.apply_constraints()
        personaDiz = self.get_persona()
        self.get_anagrafica()
        self.get_atti_per_deputato(personaDiz, annoL)
        self.get_ogni_atto(annoL)
        self.get_votazioni(anno_mese_giornoL)
        self.get_votazioni_per_deputato(espressioneL, self.get_mese_anno_list(), personaDiz)
        self.clean_data()


if __name__ == '__main__':
    oo = ImportCamera()
    oo.run()
