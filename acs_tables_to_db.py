
import sqlite3 as sql
import csv

def acs_to_db(tablename):
	db = "/home/eric/detroit_research_projects/data/HealthyHomes/HealthyHomes.sqlite"
	con = sql.connect(db)
	cur = con.cursor()

	# inFile = "/home/eric/detroit_research_projects/data/tract_data_for_evictions/{}/{}_metadata.csv".format(tablename, tablename)
	inFile = "/home/eric/detroit_research_projects/data/HealthyHomes/census/ACS2014_5Year/{}/{}_metadata.csv".format(tablename, tablename)
	with open(inFile, 'rb') as f:
		
		field_only = []
		schema = []

		reader = csv.reader(f)
		counter = 0
		for row in reader:
			field_name = row[0].upper()
			field_name = field_name.replace('.', '').replace('-','')
			dtype = None
			if counter < 3	:
				dtype = 'TEXT'
			else:
				dtype = 'REAL'
			field_only.append(field_name)
			field = (field_name, dtype)
			field = ' '.join(field)
			schema.append(field)
			counter+=1


	print schema

	cur.execute("DROP TABLE IF EXISTS {};".format(tablename))
	cur.execute("CREATE TABLE IF NOT EXISTS {} ({});".format(tablename,  ', '.join(map(str, schema))))

	# create insert template
	cur.execute("SELECT * FROM {};".format(tablename))
	fields = list([cn[0] for cn in cur.description])
	qmarks = ["?"] * len(fields)
	insert_tmpl = "INSERT INTO {} ({}) VALUES ({});".format(tablename, ', '.join(map(str, fields)),', '.join(map(str, qmarks)))

	# process census 2010 data

	# inFile = "/home/eric/detroit_research_projects/data/tract_data_for_evictions/{}/{}.csv".format(tablename, tablename)
	inFile = "/home/eric/detroit_research_projects/data/HealthyHomes/census/ACS2014_5Year/{}/{}.csv".format(tablename, tablename)
	with open(inFile, 'rb') as f:
		reader = csv.reader(f)
		header = reader.next()
		for row in reader:
			cur.execute(insert_tmpl, row)

	con.commit()
	print "{} changes made".format(con.total_changes)

	cur.execute("CREATE INDEX idx_geoid_{} ON {}(GEOID2);".format(tablename, tablename))
	con.commit()

	con.close()

################################################################################################
# read acs data to db
table_dict = {
	'ACS_14_5YR_B01001'	: 'age',
	'ACS_14_5YR_B03002'	: 'race',
	'ACS_14_5YR_B17001'	: 'poverty status',
	'ACS_14_5YR_B25003'	: 'tenure',	
	'ACS_14_5YR_B25034'	: 'year structure built',	
}

for k, v in table_dict.iteritems():
	print k, v
	acs_to_db(k)
	print "+" * 100

print "DONE"