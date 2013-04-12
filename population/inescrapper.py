#encoding: utf-8

import csv
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import date

from inemodel import *

engine = create_engine('sqlite:///ine.db', convert_unicode=True, pool_recycle=3600)

here = os.path.dirname(__file__)

def ine_scrapper(path, year):
    inelist = []
    base = []

    with open(path, 'r') as stdoc:
        reader = csv.reader(stdoc, delimiter=',', quoting=csv.QUOTE_NONE)
        for i, details in enumerate(reader):
            if len(details) > 1:
                inedict = {}
                if i == 7:
                    details[0] = 'municipality'
                    for title in details:
                        if title == '85 y más':
                            title = '85+'
                        base.append(title)
                        inedict[title] = ''
                elif i in range(9, 121):
                    for j, datum in enumerate(details):
                        inedict[base[j]] = details[j]

                    eightifivecuration = None
                    for key in inedict.keys():
                        if key == 'municipality':
                            cp = inedict[key][3:8]
                            municipality = inedict[key][9:]
                            inedict['municipality'] = municipality
                            inedict['cp'] = cp
                       
                        if key in ['85-89', '90-94', '95-99', '100 y más']:
                            if eightifivecuration is None:
                                eightifivecuration = 0
                            eightifivecuration += int(inedict[key])
                            del inedict[key]

                    if eightifivecuration is not None:
                        inedict['85+'] = str(eightifivecuration)

                    inedict['year'] = str(year)

                    inelist.append(inedict)

    return inelist

Base.metadata.create_all(bind = engine)

Session = sessionmaker(bind = engine)
session = Session()

for year in range(2008, 2012):
    #csvpath = here + '/' + str(year) + '.csv'
    csvpath = str(year) + '.csv'

    for inedict in ine_scrapper(csvpath, year):
        
        for key, value in inedict.items():
            if key not in ['year', 'municipality', 'cp']:
                municipality = session.query(Municipality).filter_by(cp = inedict['cp']).first()
                if municipality is None:
                    municipality = Municipality(cp=int(inedict['cp']), name=unicode(inedict['municipality'], 'utf-8'))
                    session.add(municipality)
                    session.commit()
                    print "[Municipality] Added " + str(municipality.cp)

                if len(key.split('-')) > 1:
                    age_low_limit = key.split('-')[0]
                    age_up_limit = key.split('-')[1]
                else:
                    age_low_limit = key.split('-')[0][:-1]
                    age_up_limit = '-1'

                population = Population(year=date(year, 1, 1), age_low_limit=int(age_low_limit), age_up_limit=int(age_up_limit), population=int(value), municipality=municipality)
                session.add(population)
                session.commit()
                print "[Population] Added " + str(municipality.cp) + ' ' + str(year) + ' ' + key

session.close()