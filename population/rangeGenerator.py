def res(agerange):
	resstr = '''
<http://helheim.deusto.es/linkedstats/resource/code/ageRange/''' + agerange + '''> a skos:Concept, pbmd-stats:AgeRange;
	skos:topConceptOf stats-code:ageRange;
	skos:inScheme     stats-code:ageRange;
	skos:prefLabel "Age range ''' + agerange + '''"@en ;
	skos:label "Age range ''' + agerange + '''"@en ;
	skos:notation "''' + agerange + '''" .
stats-code:ageRange skos:hasTopConcept <http://helheim.deusto.es/linkedstats/resource/code/ageRange/''' + agerange + '''> .'''
	return resstr

for a in sorted(['0-4', '30-34', '35-39', '25-29', '10-14', '40-44', '80-84', '50-54', '55-59', '70-74', '75-79', '45-49', '65-69', '15-19', '60-64', '20-24', '5-9']):
	print res(a)