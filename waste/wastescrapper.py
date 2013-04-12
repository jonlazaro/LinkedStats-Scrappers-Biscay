#encoding: utf-8

import csv
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import date

from wastemodel import *

engine = create_engine('sqlite:///waste.db', convert_unicode=True, pool_recycle=3600)

here = os.path.dirname(__file__)

def waste_scrapper(path, year):
    wastelist = []

    with open(path, 'r') as stdoc:
        reader = csv.reader(stdoc, delimiter=',', quotechar='"')#quoting=csv.QUOTE_NONE)
        for i, details in enumerate(reader):
            if len(details) > 1:
                wastedict = {}
                if i in range(1, 113):
                    wastedict['cp'] = str(int(details[0]) + 48000)
                    wastedict['municipality'] = details[1]
                    wastedict['organic'] = {'kg': details[2], 'cont': details[3]}
                    wastedict['paper'] = {'kg': details[4], 'cont': details[5]}
                    wastedict['glass'] = {'kg': details[6], 'cont': details[7]}
                    wastedict['plastic'] = {'kg': details[9], 'cont': details[10]}
                    wastedict['voluminous'] = {'kg': details[8], 'cont': '-1'}
                    wastedict['year'] = str(year)

                    wastelist.append(wastedict)

    return wastelist

Base.metadata.create_all(bind = engine)

Session = sessionmaker(bind = engine)
session = Session()

for year in range(2008, 2012):
    #csvpath = here + '/' + str(year) + '.csv'
    csvpath = str(year) + '.csv'

    for wastedict in waste_scrapper(csvpath, year):
        for key, value in wastedict.items():
            if key not in ['year', 'municipality', 'cp']:
                municipality = session.query(Municipality).filter_by(cp = wastedict['cp']).first()
                if municipality is None:
                    municipality = Municipality(cp=int(wastedict['cp']), name=unicode(wastedict['municipality'], 'utf-8'))
                    session.add(municipality)
                    session.commit()
                    print "[Municipality] Added " + str(municipality.cp)

                measure = Measure(year=date(year, 1, 1), waste_type=key, waste_kg=value['kg'], n_containers=value['cont'], municipality=municipality)
                session.add(measure)
                session.commit()
                print "[Measure] Added " + str(municipality.cp) + ' ' + str(year) + ' ' + key