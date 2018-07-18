import sqlite3 as sql
import csv

inFile = "/bigdrive/healthy_homes/data/health_data/toxic_structures15_061118.csv"
with open(inFile, 'rb') as f:
	reader = csv.reader(f,  delimiter=';')
	header = reader.next()
	first = reader.next()

	for i, x in enumerate(zip(header, first)):
		print i, x

# db = "/bigdrive/healthy_homes/data/healthy_homes.sqlite"
db = "/home/eric/detroit_research_projects/data/HealthyHomes/HealthyHomes.sqlite"
con = sql.connect(db)
cur = con.cursor()

# create table
cur.execute("DROP TABLE IF EXISTS test_data_2015;")

cur.execute('''
	CREATE TABLE test_data_2015 (
	parcel TEXT PRIMARY KEY,
	test_date TEXT,
	sex TEXT,
	infant TEXT,
	bll INT
	);
	''')

with open(inFile, 'rb') as f:
	reader = csv.reader(f,  delimiter=';')
	header = reader.next()
	for row in reader:
		# convert date to sqlite format (YYYY-MM-DD)
		date_orig = row[1]
		date_list = date_orig.split(" ")[0] # break at first space to collect date info
		date_list = date_list.split("/") # break at slashes to sep date fields
		year = date_list[2]
		month = date_list[0]
		day = date_list[1]
		# add leading 0s
		if len(month) == 1:
			month = "0" + month
		if len(day) == 1:
			day = "0" + day
		# reassemble date
		row[1] = year + "-" + month + "-" + day
		# insert data into table
		cur.execute('''
			INSERT INTO test_data_2015 (parcel, test_date, sex, infant, bll)
			VALUES (?, ?, ?, ?, ?);
			''', (row))

con.commit()

print "{} changes made".format(con.total_changes)

con.close()