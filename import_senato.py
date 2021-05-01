from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import time


class ImportSenato:
    def __init__(self):
        neo_url = r"bolt://localhost:7687"
        self.neo4j_client = self.get_neo_client(neo_url)
        self.sparql = SPARQLWrapper(r'http://dati.camera.it/sparql')

    def read_query_file(self, query_file):
        with open(query_file, "r") as f:
            return f.read()

    def get_neo_client(self, url):
        driver = GraphDatabase.driver(encrypted=False, uri=f"{url}", auth=("neo4j", "senato"))
        return driver

    def ingest_into_neo4j(self, query):
        with self.neo4j_client.session() as session:
            session.run(query)

    def get_anagrafica(self):
        lista_deputati = []
        query_anagrafica_sparql = self.read_query_file(os.path.join( os.path.dirname(__file__),'query_anagrafica.sparql'))
        query_anagrafica_cypher = self.read_query_file(os.path.join( os.path.dirname(__file__),'query_anagrafica.cypher'))
        self.sparql.setQuery(query_anagrafica_sparql)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        print("INIZIO CARICAMENTO ANAGRAFICA")
        for result in results["results"]["bindings"]:
            query = query_anagrafica_cypher.format(persona = result['persona']['value'],luogoNascita=result['luogoNascita']['value'],
                                                 collegio = result['collegio']['value'], lista=result.get('sigla.value',''), commissione=result.get('commissione.value',''),
                                                 numeroMandati = result['numeroMandati']['value'], genere= result['genere']['value'],info = result.get('info.value', ''),
                                                 nomeGruppo = result['nomeGruppo']['value'], nome = result['nome']['value'], cognome = result['cognome']['value'],
                                                 dataNascita = result['dataNascita']['value'], inizioMandato = result['inizioMandato']['value'],
                                                 fineMandato = result.get('fineMandato.value', '' ) )
            self.ingest_into_neo4j(query)
            lista_deputati.append({"cognome": result['cognome']['value'],"nome":result['nome']['value']})
        print("FINE CARICAMENTO ANAGRAFICA")
        return lista_deputati

    def get_atti_per_deputato(self, deputatiL, annoL):
        query_atti_sparql = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_atti.sparql'))
        query_atti_cypher = self.read_query_file(os.path.join(os.path.dirname(__file__), 'query_atti.cypher'))
        for deputato in deputatiL:
            for anno in annoL:
                param_query = query_atti_sparql.format(cognome=deputato['cognome'],nome=deputato['nome'],anno=anno)
                print(param_query)
                self.sparql.setQuery(param_query)
                self.sparql.setReturnFormat(JSON)
                results = self.sparql.query().convert()

                print(f"INIZIO CARICAMENTO ATTI PER DEPUTATO {deputato['nome']} {deputato['cognome']} per l'anno {anno[1:]}")
                if results["results"]["bindings"]:
                    for result in results["results"]["bindings"]:
                        query = query_atti_cypher.format(nome = result['nome']['value'], cognome = result['cognome']['value'], atto = result['atto']['value'],
                                                         titolo = result['titolo']['value'].replace("'","\\'"), numeroAtto = result['numeroAtto']['value'], tipo = result['tipo']['value'],
                                                         tipoRuolo = result['tipoRuolo']['value'], date = result['date']['value'] )
                        self.ingest_into_neo4j(query)
                print(f"FINE CARICAMENTO ATTI PER DEPUTATO {deputato['nome']} {deputato['cognome']} per l'anno {anno[1:]}")
                time.sleep(3)



    def apply_constraints(self):
        query_constraints_cypher = self.read_query_file(os.path.join( os.path.dirname(__file__),'create_constraints.cypher'))
        for line in query_constraints_cypher.split("\n"):
            print(line)
            self.ingest_into_neo4j(line)
        print("FINE CARICAMENTO CONSTRAINTS")



    def run(self):
        annoL = ["^2018","^2019","^2020","2021"]
        self.apply_constraints()
        deputatiL = self.get_anagrafica()
        self.get_atti_per_deputato(deputatiL, annoL)





if __name__ == '__main__':
    oo = ImportSenato()
    oo.run()
    del oo