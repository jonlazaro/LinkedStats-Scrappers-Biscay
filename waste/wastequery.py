#encoding: utf-8

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from wastemodel import *

import operator
import json
import urllib2

engine = create_engine('sqlite:///waste.db', convert_unicode=True, pool_recycle=3600)
engine2 = create_engine('sqlite:///ine.db', convert_unicode=True, pool_recycle=3600)

Session = sessionmaker(bind = engine)
Session2 = sessionmaker(bind = engine2)
session = Session()
session2 = Session2()

measure_regs = session.query(Measure).all()

turtlerdf = """@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix qb: <http://purl.org/linked-data/cube#> .

@prefix stats-dimension: <http://helheim.deusto.es/linkedstats/resource/dimension/> .
@prefix stats-measure: <http://helheim.deusto.es/linkedstats/resource/measure/> .
@prefix stats-dataset: <http://helheim.deusto.es/linkedstats/resource/dataset/> .
"""

def get_geonames_uri(municipality_name):
    geonamesapiurl = 'http://api.geonames.org/search?q=' + urllib2.quote(municipality_name) + '&maxRows=10&featureClass=A&country=ES&type=json&username=morelab'
    j = json.load(urllib2.urlopen(geonamesapiurl))
    if int(j['totalResultsCount']) >= 1:
        return 'http://sws.geonames.org/' + str(j['geonames'][0]['geonameId']) + '/'
    else:
        return ''


uridict = {}
totdict = {}
n_geonames_count = 0

for p in measure_regs:
    inemunicipality = session2.query(Municipality).filter_by(cp=p.municipality.cp).first()

    n_containers = p.n_containers if p.n_containers != -1 else 0
    n_containers = 0 if n_containers == u'' else n_containers
    geonamesuri = ''
    if inemunicipality.cp in uridict:
        geonamesuri = uridict[inemunicipality.cp]
    else:
        n_geonames_count += 1
        print n_geonames_count
        geonamesuri = get_geonames_uri(inemunicipality.name.encode('utf-8'))
        uridict[inemunicipality.cp] = geonamesuri

    m_rdf = '''
<http://helheim.deusto.es/linkedstats/resource/waste/''' + str(p.year.year) + "/" + str(inemunicipality.cp) + "/" + str(p.waste_type) + '''> a qb:Observation;
    qb:dataSet stats-dataset:waste;
    stats-dimension:year <http://reference.data.gov.uk/id/year/''' + str(p.year.year) + '''>;
    stats-dimension:municipality <''' + str(geonamesuri) + '''>;
    stats-dimension:wasteType <http://helheim.deusto.es/linkedstats/resource/code/wasteType/''' + str(p.waste_type) + '''>;
    stats-measure:wasteKg "''' + str(p.waste_kg) + '''"^^xsd:decimal;
    stats-measure:nContainers "''' + str(n_containers) + '''"^^xsd:integer;
    rdfs:label "Collected waste mass and number of containers of ''' + str(p.waste_type) + ' at ' + inemunicipality.name.encode('utf-8') + ' on year ' + str(p.year.year) + '''."@en;
    rdfs:label "Masa de residuos recogidos y número de containers de ''' + str(p.waste_type) + ' en ' + inemunicipality.name.encode('utf-8') + ' en el año ' + str(p.year.year) + '''."@es;
    rdfs:label "''' + inemunicipality.name.encode('utf-8') + '-n ' + str(p.waste_type) + '-zko batutako masa eta kontainer kopurua ' + str(p.year.year) + '.ean."@eu .'

    if geonamesuri != '':
        turtlerdf += m_rdf

        dicttuple = (inemunicipality.cp, 'http://reference.data.gov.uk/id/year/' + str(p.year.year))
        
        if dicttuple in totdict:
            totdict[dicttuple] = tuple(map(operator.add, totdict[dicttuple], (p.waste_kg, n_containers)))
        else:
            totdict[dicttuple] = (p.waste_kg, n_containers)

    else:
        print 'GEONAMES ERROR!'
        print m_rdf
        print '-'*20


#TOTALS
for key, value in totdict.items():
    cp = key[0]
    anyo = str(key[1].replace('http://reference.data.gov.uk/id/year/', ''))
    municip_name = session2.query(Municipality).filter_by(cp=cp).first().name.encode('utf-8')

    turtlerdf += '''
<http://helheim.deusto.es/linkedstats/resource/waste/''' + str(anyo) + "/" + str(cp) + '''/total> a qb:Observation;
    qb:dataSet stats-dataset:waste;
    stats-dimension:year <''' + key[1] + '''>;
    stats-dimension:municipality <''' + uridict[cp] + '''>;
    stats-dimension:wasteType <http://helheim.deusto.es/linkedstats/resource/code/wasteType/total>;
    stats-measure:wasteKg "''' + str(value[0]) + '''"^^xsd:decimal;
    stats-measure:nContainers "''' + str(value[1]) + '''"^^xsd:integer;
    rdfs:label "Total collected waste mass and number of containers at ''' + str(municip_name) + ' on year ' + str(anyo) + '''."@en;
    rdfs:label "Masa de residuos recogidos y número de containers total en ''' + str(municip_name) + ' en el año ' + str(anyo) + '''."@es;
    rdfs:label "''' + str(municip_name) + '-n ' + 'batutako masa eta kontainer kopuru totala ' + str(anyo) + '.ean."@eu .'

f = open('waste.ttl', 'w')
f.write(turtlerdf)
f.close()