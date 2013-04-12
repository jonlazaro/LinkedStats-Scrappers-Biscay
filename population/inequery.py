#encoding: utf-8

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from inemodel import *

import json
import urllib2

engine = create_engine('sqlite:///ine.db', convert_unicode=True, pool_recycle=3600)

Session = sessionmaker(bind = engine)
session = Session()

population_regs = session.query(Population).all()
#pop = session.query(Population).first()

turtlerdf = """@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix qb: <http://purl.org/linked-data/cube#> .

@prefix stats-dimension: <http://helheim.deusto.es/linkedstats/resource/dimension/> .
@prefix stats-measure: <http://helheim.deusto.es/linkedstats/resource/measure/> .
@prefix stats-dataset: <http://helheim.deusto.es/linkedstats/resource/dataset/> .
"""


def get_geonames_uri(municipality_name):
    geonamesapiurl = 'http://api.geonames.org/search?q=' + urllib2.quote(municipality_name) + '&maxRows=10&featureClass=A&country=ES&type=json&username=jonla'
    j = json.load(urllib2.urlopen(geonamesapiurl))
    if int(j['totalResultsCount']) >= 1:
        return 'http://sws.geonames.org/' + str(j['geonames'][0]['geonameId']) + '/'
    else:
        return ''


uridict = {}
totdict = {}
n_geonames_count = 0

#population_regs = [pop]
for p in population_regs:
    #p = session.query(Population).first()
    agerange = str(p.age_low_limit) + '-' + str(p.age_up_limit) if p.age_up_limit != -1 else str(p.age_low_limit) + '-and-over'

    geonamesuri = ''
    if p.municipality.cp in uridict:
        geonamesuri = uridict[p.municipality.cp]
    else:
        n_geonames_count += 1
        print n_geonames_count
        geonamesuri = get_geonames_uri(p.municipality.name.encode('utf-8'))
        uridict[p.municipality.cp] = geonamesuri

    p_rdf = '''
<http://helheim.deusto.es/linkedstats/resource/population/''' + str(p.year.year) + "/" + str(p.municipality.cp) + "/" + agerange + '''> a qb:Observation;
    qb:dataSet stats-dataset:population;
    stats-dimension:year <http://reference.data.gov.uk/id/year/''' + str(p.year.year) + '''>;
    stats-dimension:municipality <''' + str(geonamesuri) + '''>;
    stats-dimension:ageRange <http://helheim.deusto.es/linkedstats/resource/code/ageRange/''' + agerange + '''>;
    stats-measure:population "''' + str(p.population) + '''"^^xsd:integer;
    rdfs:label "Population of age range ''' + agerange + ' at ' + p.municipality.name.encode('utf-8') + ' on year ' + str(p.year.year) + '''."@en;
    rdfs:label "Populación del rango de edad ''' + agerange + ' en ' + p.municipality.name.encode('utf-8') + ' en el año ' + str(p.year.year) + '''."@es;
    rdfs:label "''' + p.municipality.name.encode('utf-8') + '-ko populazioa ' + str(p.year.year) + '.ean ' + agerange + 'adin-tartean."@eu .'

    if geonamesuri != '':
        turtlerdf += p_rdf
        if (p.municipality.cp, 'http://reference.data.gov.uk/id/year/' + str(p.year.year)) in totdict:
            totdict[(p.municipality.cp, 'http://reference.data.gov.uk/id/year/' + str(p.year.year))] += p.population
        else:
            totdict[(p.municipality.cp, 'http://reference.data.gov.uk/id/year/' + str(p.year.year))] = p.population
    else:
        print 'GEONAMES ERROR!'
        print p_rdf
        print '-'*20

#TOTALS
for key, value in totdict.items():
    cp = key[0]
    anyo = str(key[1].replace('http://reference.data.gov.uk/id/year/', ''))
    municip_name = session.query(Municipality).filter_by(cp=cp).first().name.encode('utf-8')

    turtlerdf += '''
<http://helheim.deusto.es/linkedstats/resource/population/''' + str(anyo) + "/" + str(cp) + '''/total> a qb:Observation;
    qb:dataSet stats-dataset:population;
    stats-dimension:year <''' + key[1] + '''>;
    stats-dimension:municipality <''' + uridict[cp] + '''>;
    stats-dimension:ageRange <http://helheim.deusto.es/linkedstats/resource/code/ageRange/total>;
    stats-measure:population "''' + str(value) + '''"^^xsd:integer;
    rdfs:label "Total population at ''' + str(municip_name) + ' on year ' + str(anyo) + '''."@en;
    rdfs:label "Populación total en ''' + str(municip_name) + ' en el año ' + str(anyo) + '''."@es;
    rdfs:label "''' + str(municip_name) + '-ko populazio totala ' + str(anyo) + '.ean "@eu .'

print len(totdict)

#print turtlerdf
f = open('ine.ttl', 'w')
f.write(turtlerdf)
f.close()

#stats-dimension:municipality <http://helheim.deusto.es/bizkaisense/resource/municipality/''' + str(p.municipality.cp) + '''>;