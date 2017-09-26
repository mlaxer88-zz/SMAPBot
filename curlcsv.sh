## Shell script to grab CSV from SMAP SDS site to use with SMAPBot


CSV=$(curl -s 'https://smap.jpl.nasa.gov/user-products/bad-missing-data/' | grep csv | awk -F '"' '{print$2}')
BASEURL="https://smap.jpl.nasa.gov"
URL=$BASEURL$CSV

curl -o master_list.csv $URL
