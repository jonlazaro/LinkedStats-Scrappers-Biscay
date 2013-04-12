from sqlalchemy import Column, Integer, Unicode, Float, Date, ForeignKey, UniqueConstraint, Boolean
from sqlalchemy.orm import relation, backref
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import os

#SQLALCHEMY_ENGINE_STR = 'mysql://basuras:basuras@127.0.0.1/rhok_desahucios'

#engine = create_engine(SQLALCHEMY_ENGINE_STR, convert_unicode=True, pool_recycle=3600)

engine = create_engine('sqlite:///ine.db', convert_unicode=True, pool_recycle=3600)

Base = declarative_base()

class Municipality(Base):
	__tablename__ = 'Municipalities'
	id = Column(Integer, primary_key = True)
	cp = Column(Integer)
	name = Column(Unicode(200))

	def __init__(self, cp, name):
		self.cp = cp
		self.name = name

class Population(Base):
	__tablename__ = 'Populations'
	id = Column(Integer, primary_key = True)
	year = Column(Date())
	age_low_limit = Column(Integer)
	age_up_limit = Column(Integer)
	population = Column(Integer)
	municipality_id = Column(Integer, ForeignKey('Municipalities.id'), nullable = False)
	municipality = relation(Municipality.__name__, backref = backref('measures', order_by=id, cascade = 'all,delete'))

	def __init__(self, year, age_low_limit, age_up_limit, population, municipality):
		self.year = year
		self.age_low_limit = age_low_limit
		self.age_up_limit = age_up_limit
		self.population = population
		self.municipality = municipality