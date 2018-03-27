# convert assessor data in ESRI file geodatabase to spatialite db
ogr2ogr -f "SQLite" -dsco SPATIALITE=YES -t_srs EPSG:2898 \
/home/eric/detroit_research_projects/data/HealthyHomes/HealthyHomes.sqlite \
/home/eric/detroit_research_projects/data/assessor/DetroitAssessorBA.gdb/

ogr2ogr -f "SQLite" -update -t_srs EPSG:2898 \
/home/eric/detroit_research_projects/data/HealthyHomes/HealthyHomes.sqlite \
/home/eric/detroit_research_projects/data/assessor/assessor2015/ParcelMap.shp \
-nlt PROMOTE_TO_MULTI -nln DetroitParcels2015

ogr2ogr -f "SQLite" -update -t_srs EPSG:2898 \
/home/eric/detroit_research_projects/data/HealthyHomes/HealthyHomes.sqlite \
/home/eric/detroit_research_projects/data/DetroitOpenData/parcel_points_ownership_2017-06-17 \
-nlt PROMOTE_TO_MULTI -nln DetroitParcels2017

ogr2ogr -f "SQLite" -update -t_srs EPSG:2898 \
/home/eric/detroit_research_projects/data/HealthyHomes/HealthyHomes.sqlite \
/home/eric/detroit_research_projects/data/derived_data/detroit_spatial.sqlite det_trt_2010

ogr2ogr -f "SQLite" -update -t_srs EPSG:2898 \
/home/eric/detroit_research_projects/data/HealthyHomes/HealthyHomes.sqlite \
/home/eric/detroit_research_projects/data/tax_foreclosure/Archival_Tax_Foreclosures_in_Detroit_2002__2013/ \
-nln taxforeclosure0213