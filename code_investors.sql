# Find investor-owned properties 2009
SELECT COUNT(*) FROM
(
SELECT A.parcelnumber, 
A.propaddresscombined, 
A.taxpayername1, 
A.taxablestatus,
A.resyearbuilt,
B.props
FROM 
detroitparcels2009 AS A
JOIN (SELECT taxpayername1, COUNT(*) AS props
	FROM detroitparcels2009
	WHERE taxpayername1 IS NOT NULL 
	AND taxpayername1 NOT IN ('TAXPAYER', 'TAXPAYER/OCCUPANT')
	AND propertyclass = '401'
	AND resnumbldgs >= 1
	AND taxablestatus = 'TAXABLE'
	GROUP BY taxpayername1
	HAVING COUNT(*) >= 5
	) AS B ON A.taxpayername1 = B.taxpayername1
WHERE A.taxablestatus = 'TAXABLE'
AND A.propertyclass = '401'
AND A.resnumbldgs >= 1
)
;



# Find investor-owned properties 2017
SELECT COUNT(*) FROM
(
SELECT A.parcelno, 
A.propaddr, 
A.taxpayer1, 
A.taxstatus,
A.resyrbuilt,
B.props
FROM 
detroitparcels2017 AS A
JOIN (SELECT taxpayer1, COUNT(*) AS props
	FROM detroitparcels2017
	WHERE taxpayer1 IS NOT NULL 
	AND taxpayer1 NOT IN ('TAXPAYER', 'TAXPAYER/OCCUPANT')
	AND propclass = '401'
	AND resbldgno >= 1
	AND taxstatus = 'TAXABLE'
	GROUP BY taxpayer1
	HAVING COUNT(*) >= 5
	) AS B ON A.taxpayer1 = B.taxpayer1
WHERE A.taxstatus = 'TAXABLE'
AND A.propclass = '401'
AND A.resbldgno >= 1
)
;