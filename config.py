
# where to get the data from
BUSINESS_ENTITIES_URL = "https://data.colorado.gov/resource/4ykn-tg5h.json?$query=SELECT%0A%20%20%60entityformdate%60%2C%0A%20%20count(%60entityid%60)%20AS%20%60count_entityid%60%2C%0A%20%20%60principalcity%60%2C%0A%20%20%60entitystatus%60%2C%0A%20%20%60principalzipcode%60%0AGROUP%20BY%0A%20%20%60entityformdate%60%2C%0A%20%20%60principalcity%60%2C%0A%20%20%60entitystatus%60%2C%0A%20%20%60principalzipcode%60%0AORDER%20BY%20%60entityformdate%60%20LIMIT%2010000000"
BUSINESS_STATISTICS_URL = "https://www.census.gov/econ_export/?format=csv&mode=report&default=false&errormode=Dep&charttype=&chartmode=&chartadjn=&submit=GET+DATA&program=BFS&startYear=2004&endYear=2024&categories%5B0%5D=TOTAL&dataType=BA_BA&geoLevel=CO&adjusted=false&notAdjusted=true&errorData=false"

# where to save the data
BUSINESS_ENTITIES_CSV_PATH = 'data/business_entities.csv'
BUSINESS_STATISTICS_CSV_PATH = 'data/business_statistics.csv'

