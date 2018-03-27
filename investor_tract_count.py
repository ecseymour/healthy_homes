'''
calculate tract-year counts of investor-owned homes
don't think i can loop over years because field names vary
for each year, find parcels owned by actor with 5+ parcels

do i want to add column coding properties as investor owned
or create temp table, dict, etc.? prob want to update source
table so records may be included in spatial query
#################################################
start with 2014 and 2015 data
#################################################
'''
from pysqlite2 import dbapi2 as sql
from collections import OrderedDict
import pandas as pd
import geopandas as gpd
import datetime
from matplotlib import pyplot as plt

dbFile = "/home/eric/detroit_research_projects/data/HealthyHomes/HealthyHomes.sqlite"
con = sql.connect(dbFile)
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()
##############################################################################
##############################################################################
def add_investor_binary_field():
	for y in ['2014', '2015']:
		cur.execute("pragma table_info(detroitparcels{});".format(y))
		field_check = False	
		results = cur.fetchall()
		for row in results:
			field_name = row[1]
			if field_name == 'investor_flag':
				field_check = True
		if field_check == False:
			print y, field_check
			cur.execute("ALTER TABLE detroitparcels{} ADD COLUMN investor_flag INTEGER;".format(y))
			con.commit()
		else:
			pass

# add_investor_binary_field()
##############################################################################
##############################################################################
def code_investors_2014():
	# overwrite past updates
	cur.execute("UPDATE detroitparcels2014 SET investor_flag = NULL;")
	con.commit()
	# write new updates
	investor_dict = {}
	cur.execute('''
		SELECT A.rowid 
		FROM detroitparcels2014 AS A
		JOIN (SELECT ownername, COUNT(*) AS props
			FROM detroitparcels2014
			WHERE ownername IS NOT NULL 
			AND ownername NOT IN ('TAXPAYER', 'TAXPAYER/OCCUPANT')
			AND propertyclass = '401'
			AND resnumbldgs >= 1
			AND taxablestatus = 'TAXABLE'
			GROUP BY ownername
			HAVING COUNT(*) >= 5
			) AS B ON A.ownername = B.ownername
		WHERE A.taxablestatus = 'TAXABLE'
		AND A.propertyclass = '401'
		AND A.resnumbldgs >= 1
		''')
	results = cur.fetchall()
	for row in results:
		rowid = row[0]
		investor_dict[rowid] = 1

	for k, v in investor_dict.iteritems():
		cur.execute("UPDATE detroitparcels2014 SET investor_flag = 1 WHERE rowid = ?;",([k]) )

	con.commit()
	print "{} changes".format(con.total_changes)

# code_investors_2014()
##############################################################################
##############################################################################
'''
for each tract, collect numerator (investor counts)
and denominator (total residential structure) counts
'''
def get_tract_counts():
	tract_dict = {}
	cur.execute("SELECT geoid10 FROM det_trt_2010;")
	results = cur.fetchall()
	for row in results:
		tract = row[0]
		tract_dict[tract] = {'numerator' : 0, 'denominator' : 0, 'rate': 0}
	for k, v in tract_dict.iteritems():
		cur.execute('''
			SELECT COUNT(*)
			FROM detroitparcels2014 AS A, det_trt_2010 AS B
			WHERE ST_Contains(B.geometry, A.geometry)
			AND A.ROWID IN (SELECT ROWID FROM SpatialIndex
				WHERE f_table_name='detroitparcels2014' and search_frame=B.geometry)
			AND A.investor_flag = 1 
			AND B.geoid10 = ?; 
			''', ([k]))
		results = cur.fetchone()
		if results is not None:
			numerator = results[0]
			tract_dict[k]['numerator'] += numerator

		cur.execute('''
			SELECT COUNT(*)
			FROM detroitparcels2014 AS A, det_trt_2010 AS B
			WHERE ST_Contains(B.geometry, A.geometry)
			AND A.ROWID IN (SELECT ROWID FROM SpatialIndex
				WHERE f_table_name='detroitparcels2014' and search_frame=B.geometry)
			AND A.propertyclass = '401'
			AND A.resnumbldgs >= 1
			AND B.geoid10 = ?; 
			''', ([k]))
		results = cur.fetchone()
		if results is not None:
			denominator = results[0]
			tract_dict[k]['denominator'] += denominator

		if numerator >= 1:
			tract_dict[k]['rate'] = numerator * 1.0 / denominator * 100

	df = pd.DataFrame.from_dict(tract_dict, orient='index')
	df.to_pickle("/home/eric/detroit_research_projects/data/HealthyHomes/InvestorTractCount2014.pkl")
	df.to_csv("/home/eric/detroit_research_projects/data/HealthyHomes/InvestorTractCount2014.csv", index_label='geoid10')
##############################################################################
##############################################################################
'''
start with quick count of 2013 tax foreclosure
'''
def tax_foreclosure_counts():
	tract_dict = {}
	cur.execute("SELECT geoid10 FROM det_trt_2010;")
	results = cur.fetchall()
	for row in results:
		tract = row[0]
		tract_dict[tract] = {'fc2012': 0, 'fc2013': 0}
	for k, v in tract_dict.iteritems():	
		cur.execute('''
			SELECT COUNT(*) 
			FROM taxforeclosure0213 AS A, det_trt_2010 AS B
			WHERE ST_Contains(B.geometry, A.geometry)
			AND A.ROWID IN (SELECT ROWID FROM SpatialIndex
				WHERE f_table_name='taxforeclosure0213' AND search_frame=B.geometry)
			AND fc_2013 = 1
			AND B.geoid10 = ?;
			''', ([k]))
		results = cur.fetchone()
		if results is not None:
			foreclosures = results[0]
			tract_dict[k]['fc2013'] = foreclosures

		cur.execute('''
			SELECT COUNT(*) 
			FROM taxforeclosure0213 AS A, det_trt_2010 AS B
			WHERE ST_Contains(B.geometry, A.geometry)
			AND A.ROWID IN (SELECT ROWID FROM SpatialIndex
				WHERE f_table_name='taxforeclosure0213' AND search_frame=B.geometry)
			AND fc_2012 = 1
			AND B.geoid10 = ?;
			''', ([k]))
		results = cur.fetchone()
		if results is not None:
			foreclosures = results[0]
			tract_dict[k]['fc2012'] = foreclosures

	df = pd.DataFrame.from_dict(tract_dict, orient='index')
	print df.describe()
	df.to_pickle("/home/eric/detroit_research_projects/data/HealthyHomes/TaxForeclosuresTract2013.pkl")
##############################################################################
##############################################################################
'''
merge tract-level health, acs, and investor data
read in health and investor data as dataframes
to join to acs data
'''
def merge_data():
	qry = '''
	SELECT tract.geoid10,
	age.HD01_VD01 AS pop,
	age.HD01_VD03 + age.HD01_VD27 AS under5,
	(age.HD01_VD03 + age.HD01_VD27) * 1.0 / age.HD01_VD01 * 100 AS pctunder5,
	(age.HD01_VD20 + age.HD01_VD21 + age.HD01_VD22 + age.HD01_VD23 + age.HD01_VD24 + age.HD01_VD25 + 
	age.HD01_VD44 + age.HD01_VD45 + age.HD01_VD46 + age.HD01_VD47 + age.HD01_VD48 + age.HD01_VD49) * 1.0 / age.HD01_VD01 * 100 AS pct65plus,
	race.HD01_VD04 * 1.0 / race.HD01_VD01 * 100 AS pctnhblack,
	race.HD01_VD12 * 1.0 / race.HD01_VD01 * 100 AS pcthisp,
	pov.HD01_VD02 * 1.0 / pov.HD01_VD01 * 100 AS povrate,
	tenure.HD01_VD03 * 1.0 / tenure.HD01_VD01 * 100 AS rentership,
	(yrblt.HD01_VD08 + yrblt.HD01_VD09 + yrblt.HD01_VD10 ) * 1.0 / yrblt.HD01_VD01 * 100 AS pctbefore1960,
	(yrblt.HD01_VD06 + yrblt.HD01_VD07 ) * 1.0 / yrblt.HD01_VD01 * 100 AS pct6079,
	-- (yrblt.HD01_VD06 +  yrblt.HD01_VD07 + yrblt.HD01_VD08 + yrblt.HD01_VD09 + yrblt.HD01_VD10 ) * 1.0 / yrblt.HD01_VD01 * 100 AS pct1979under,
	Hex(ST_AsBinary(tract.geometry)) AS geometry
	FROM det_trt_2010 AS tract 
	JOIN ACS_14_5YR_B01001 AS age ON tract.geoid10 = age.geoid2
	JOIN ACS_14_5YR_B03002 AS race ON age.geoid2 = race.geoid2
	JOIN ACS_14_5YR_B17001 AS pov ON age.geoid2 = pov.geoid2
	JOIN ACS_14_5YR_B25003 AS tenure ON age.geoid2 = tenure.geoid2
	JOIN ACS_14_5YR_B25034 AS yrblt ON age.geoid2 = yrblt.geoid2
	WHERE age.HD01_VD01 >= 1
	AND age.HD01_VD03 + age.HD01_VD27 >= 1
	;
	'''
	df = gpd.GeoDataFrame.from_postgis(qry, con, geom_col='geometry', index_col='geoid10')	

	# get investor data
	df2 = pd.read_pickle("/home/eric/detroit_research_projects/data/HealthyHomes/InvestorTractCount2014.pkl")

	merged = pd.merge(df, df2, left_index=True, right_index=True)
	# drop tracts w/ too few structures 
	merged = merged.loc[merged['denominator']>=100]
	merged.rename(index=str, columns={'rate': 'investor_rate'}, inplace=True)

	# collect tax foreclosure data
	df3 = pd.read_pickle("/home/eric/detroit_research_projects/data/HealthyHomes/TaxForeclosuresTract2013.pkl")
	merged = pd.merge(merged, df3, left_index=True, right_index=True)
	merged['fc13rate'] = merged['fc2013'] * 1.0 / merged['denominator'] * 100
	merged['fc12rate'] = merged['fc2012'] * 1.0 / merged['denominator'] * 100

	# collect health data
	df4 = pd.read_csv("/home/eric/detroit_research_projects/data/HealthyHomes/healthyhomes_031318.csv", index_col='geoid10')
	df4.index = df4.index.astype(str)
	merged = pd.merge(merged, df4, left_index=True, right_index=True)

	# calculate rate of ebll for 2015 using acs est in denominator
	merged['ebll2014rate'] = 0 
	merged.loc[merged['under5']>=1, 'ebll2014rate'] = merged['ebll2014'] * 1.0 / merged['under5'] * 1000
	
	# export data for further analysis
	merged.to_pickle("/home/eric/detroit_research_projects/data/HealthyHomes/merged_data.pickle")
	merged.to_csv("/home/eric/detroit_research_projects/data/HealthyHomes/merged_data.csv", index_label="geoid10")

	# merged.plot.scatter('fc12rate', 'asth2014'); plt.show()

##########################################
# RUN FUNCTIONS
##########################################
# add_investor_binary_field()
# code_investors_2014()
# get_tract_counts()
# tax_foreclosure_counts()
merge_data()


con.close()

print "done"





# SCRAPS
# iterate over tracts to make sure each tract gets a count for each year
# tract_year_dict = OrderedDict()

# cur.execute("SELECT geoid10 FROM det_trt_2010;")
# results = cur.fetchall()
# for row in results:
# 	tract = row[0]
# 	for y in range(2009, 2018):
# 		tract_year_dict[(tract, y)] = {} 

# # # 2009
# # for k, v in tract_year_dict.iteritems():
# # 	if k[1]==2009:
# # 		cur.execute('''
# # 			SELECT taxpayername1
# # 			''')

##############################################################################
##############################################################################
# for k, v in tract_year_dict.iteritems():
# 	print k, v