#!/usr/bin/python
"""Import Locations from a CSV file to mp3.
"""

import sys
import csv
import json
import cgi
import uuid
import urllib
import urllib2
import logging
import datetime
from datetime import timedelta
from collections import namedtuple
from mp.importer.urlname import suggest as suggest_urlname
from van_api import *
from van_api import van_api
import psycopg2

from mp.importer.csv import LocationUpdater, TagUpdater


COORDS_CACHE = {}	  
GEONAMES_CACHE = {}					
# TODO use context and add connection and caches to context


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
    for file_cfg in CSVFILES: #  for each file type
        credentials = van_api.ClientCredentialsGrant(file_cfg['API_KEY'], file_cfg['API_SECRET'])
        api = van_api.API('api.metropublisher.com', credentials)
        fields = ['url', 'location_types']
        start_url = '/{}/locations?fields={}&rpp=100'.format(file_cfg['INSTANCE_ID'], '-'.join(fields))
        csv_file = '/var/lib/postgresql/kiarasky/kiara/importer/VA_locations/VA_weddings.csv'			 
        count = ok = skip = tags = 0 
        try:
            conn = psycopg2.connect("dbname='aux_location_db' host='/var/run/postgresql'") 			# TODO pass these also from the conf file? or use and delete always the same aux table?
            conn.autocommit = True	
        except:
            print "I am unable to connect to the database"
        cur = conn.cursor()
        table = cur.execute("select exists(select relname from pg_class where relname='seen_coords')")
        table = cur.fetchone()[0]
        if not table:
            cur.execute("CREATE TABLE seen_coords (id serial PRIMARY KEY, urlname varchar, coords varchar);")
        else:
            print "coords table already there"
        #
        gtable = cur.execute("select exists(select relname from pg_class where relname='seen_geonames')")
        gtable = cur.fetchone()[0]
        if not gtable:
            cur.execute("CREATE TABLE seen_geonames (id serial PRIMARY KEY, urlname varchar, geoname int);")
        else:
            print "geonames table already there"
        #
        for loc_dict, tagcat_dictionary in read_locations(csv_file, conn, file_cfg):
            count += 1
            url = 'http://api.metropublisher.com/' + loc_dict['urlname']				 
            namespace = uuid.NAMESPACE_URL
            url = url.encode('utf-8')
            new_url = '%s/%s/%s' % (file_cfg['API_KEY'], str(file_cfg['INSTANCE_ID']), url) 
            loc_uuid = uuid.uuid3(namespace, new_url)		# use also API_KEY and INSTANCE_ID - repeatable uuids x idempotent api 
            locupdater = LocationUpdater(api, file_cfg['INSTANCE_ID'])	
            l_status = locupdater.upsert_location(loc_dict, loc_uuid)  
            # tags
            if file_cfg['has_tags'] = True:
                tagupdater = TagUpdater(api, file_cfg['INSTANCE_ID'])	
                t_status = tagupdater.upsert_tags(tagcat_dictionary, loc_uuid)  
                if t_status == 1:
                    tags += 1
            if l_status == 1:
                ok += 1
            else:
                skip += 1    
        # close db connection
        cur.close()
        conn.close()
        logging.info('Imported {} and removed {} out of {} locations'.format(ok, skip, count))
        logging.info('Imported also {} tags'.format(tags))
        return 0					     



"""CONFIGURATION SETUP
Each client has a configuration setup including credentials
external_unique_id is the field used to univocally identify the item in the database. 
If the client data are lacking it, we: 1) import locations and assign one external_unique_id, 2) export it back, 3) the client uses that for any update
"""


CSVFILES = [
        # 0 is demo instance
        # add error message if required fields missing!
        dict(
            client='MP-template',			# we propose a csv template for which the script is ready-to-use
            uuid_import_id = 'http://client.com/import/0/',
            INSTANCE_ID = 0, 
            API_KEY = 'mxvsm129bm7RgcGRYedzLersZXGQSwQjMiyilovZL7A',
            API_SECRET = 'hSBADtfwcEnxeatj',
            GOOGLE_API_KEY = 'AIzaSyDrFNq9li1esIyHypfNh1IZ0w4FcPDeOVs',
            GEONAME_USER = 'kiarasky2015',  
            aux_database = 'aux_location_db',
            external_unique_id = 'uuid' 			# they give us an unique uuid
            namedtuple = ('MP-template',['uuid','id', 'urlname', 'title', 'phone', 'email', 'web','number', 'street', 'postalcode','city', 'fax', 'description', 'print_description', 'content', 'price', 'reservation_url','region', 'country','creation_date','image','thumbnail','video','facebook','twitter','tag1:category1|tag2:category1,category2|tag3:None'])
            ),
        dict(
            client='VALiving',
            uuid_import_id = 'http://client.com/import/0/',
            INSTANCE_ID = 0,
            API_KEY = 'mxvsm129bm7RgcGRYedzLersZXGQSwQjMiyilovZL7A',
            API_SECRET = 'hSBADtfwcEnxeatj',
            GOOGLE_API_KEY = 'AIzaSyDrFNq9li1esIyHypfNh1IZ0w4FcPDeOVs',
            GEONAME_USER = 'kiarasky2015',
            aux_database = 'aux_location_db',  
            has_tags = True
            namedtuple = ('VAlocation', ['cat_region', 'cat_category', 'title', 'street', 'city', 'postalcode', 'phone', 'web'])
            ),
           ]


CsvFile = namedtuple('client','uuid_import_id','INSTANCE_ID','API_KEY','API_SECRET','GOOGLE_API_KEY','GEONAME_USER','aux_database','decode, has_tags, namedtuple')
_default = CsvFile('required','required','required','required','required','required','required', 'required', 'UTF-8', False, None)
CSVFILES = [_default._replace(**t) for t in CSVFILES]


def read_locations(csv_file, conn, file_cfg): # TODO Modify this to use the config
    """This function reads the csv, and based on the configuration creates and yelds location dictionaries for the API
    """
    counter = 0
    tagcat_dictionary = {}
    with open(csv_file, 'rb') as csvfile:
        next(csvfile)							
        reader = csv.reader(csvfile, delimiter=',')			
        LOCS = []
        for row in reader:
            loc_dict = {} 					
            try:
                row = decode_row(row, file_cfg['decode'])
                if file_cfg['Namedtuple']:	  		
                    row = file_cfg['Namedtuple']._make(encode_utf(row)) 		# TODO use namedtuple??? no, there are mandatory fields, use those to generalize!
                else:
                    print "check conf file"
                    return None
                loc_dict['title'] = row.title or None 
                if loc_dict['title'] is None: 
                    raise NotImplementedError # skip
                if file_cfg['urlname']:											# TODO generalize with unique_id_key or use uuid?
                    urlname = row.urlname
                else:
                    urlname = suggest_urlname(row.title).lower()				# we create the urlname, TODO make sure it's uniq
                    urlname_count = 0
                    while urlname in LOCS:
                        urlname_count += 1
                        urlname = urlname + '-%s' % urlname_count
                        print 'DUPLICATE URLNAME, TRYING', urlname	
                    LOCS.append(urlname)
                loc_dict['urlname'] = urlname
                loc_dict['street'] = row.street or None
                loc_dict['pcode'] = row.postalcode or None
                loc_dict['phone'] = row.phone or None
                #
                # TODO create tagcat_dictionary {tag1:cat1,tag2:None,tag3:cat1,cat2} - depending on the config file
                #
                if row.web:
                    if not (row.web.startswith('http://') or row.web.startswith('https://')):
                        web = 'http://' + row.web
                    else:
                        web = row.web
                    loc_dict['website'] = web	
                loc_dict['created'] = loc_dict['modified'] = str(datetime.datetime.now() - timedelta(days=2))	# yesterday's date
                loc_dict['state'] = row.status or 'draft' # TODO general, get status from csv												
                address_key = ''
                gcity = row.city
                if row.street:
                    address_key = address_key + row.street.replace(' ','+') + ','
                if row.city:
                    address_key = address_key + row.city.replace(' ','+') + ','
                address_key = address_key.replace(' ', '+') 					
                gcity.replace(' ', '+')
                #
                # coordinates use a pg db as filesystem to see if already retrieved!
                cur = conn.cursor()
                c_urlname = str(loc_dict['urlname'])
                cur.execute("SELECT * FROM seen_coords where urlname = (%s)", (c_urlname,))
                result = cur.fetchone()
                if result and result[2]:						# if it's None, i can re-try, but it will give none again - TODO fix
                    coords = []
                    for i in result[2].split(','):
                        i = i.replace('"','')
                        i = i.replace('{','')
                        i = i.replace('}','')
                        coords.append(float(i))
                    loc_dict['coords'] = coords
                else:
                    coords = COORDS_CACHE.get(urlname)		
                    if coordinates is None:	
                        coords = get_coords(address_key, loc_dict['urlname'])	
                    loc_dict['coords'] = coords 	
                    COORDS_CACHE[urlname] = coords
                    cur.execute("INSERT INTO seen_coords(urlname, coords) VALUES (%s, %s)", (c_urlname, coords))
                    conn.commit()
                #
                # geonames - TODO for Api add a warning for invalid geonames so the user knows? or set default to None?
                cur.execute("SELECT * FROM seen_geonames where urlname = (%s)", (c_urlname,))
                gresult = cur.fetchone()
                if gresult:
                    geoname = gresult[2]
                else:
                    search_title = loc_dict['title'].replace(' ', '+')
                    geoname = _GEONAMES_CACHE.get(urlname)				
                    if geoname is None:
                        geoname = get_geoname(search_title,loc_dict['pcode'], loc_dict['urlname'], gcity)
                    if geoname is None:
                        print "geoname from API is None"
                    else:		 
                        _GEONAMES_CACHE[urlname] = geoname	  
                        cur.execute("INSERT INTO seen_geonames(urlname, geoname) VALUES (%s, %s)", (c_urlname, geoname))
                        conn.commit()
                loc_dict['geoname_id'] = int(geoname)
                counter += 1
                yield loc_dict, tagcat_dictionary 
            except:
                print 'Failed on {}'.format(row)
                raise



if __name__ == '__main__':
    sys.exit(main())


