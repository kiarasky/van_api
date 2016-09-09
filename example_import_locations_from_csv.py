#!/usr/bin/python
"""Import Locations from a CSV file to mp3.
"""

import sys
import csv
import json
import cgi
import uuid
import logging
import datetime
import psycopg2
from datetime import timedelta
from collections import namedtuple

import van_api

sys.path.append('/home/kiarasky/GIT/mp.importer/')
from mp.importer.csv import LocationUpdater, TagUpdater
from mp.importer.csv import get_coords, get_geoname, decode_row, encode_utf
#from mp.importer.urlname import suggest as suggest_urlname # gives error icu.InvalidArgsError: (<class 'icu.Transliterator'>, 'createInstance', ('Any-Latin; Latin ASCII',))

COORDS_CACHE = {}	  
GEONAMES_CACHE = {}					
# TODO use context and add connection and caches to context


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
    for file_cfg in CSVFILES: #  for each file type - no, this should be an argument
        credentials = van_api.ClientCredentialsGrant(file_cfg.API_KEY, file_cfg.API_SECRET)
        api = van_api.API('api.metropublisher.com', credentials)
        fields = ['url', 'location_types']
        start_url = '/{}/locations?fields={}&rpp=100'.format(file_cfg.INSTANCE_ID, '-'.join(fields))
        csv_file = file_cfg.basepath + file_cfg.csvfile		
        count = ok = skip = tags = 0 
        try:
            conn = psycopg2.connect("dbname='aux_location_db' user='kiara' host='localhost' password='kiara'") 			
            # TODO 1) create db if not there, 2) pass conn data from conf 3) can we store this elsewhere?
            conn.autocommit = True	
        except:
            print ("I am unable to connect to the database")
            break 																					# give error message
        cur = conn.cursor()
        table = cur.execute("select exists(select relname from pg_class where relname='seen_coords')")
        table = cur.fetchone()[0]
        if not table:
            cur.execute("CREATE TABLE seen_coords (id serial PRIMARY KEY, urlname varchar, coords varchar);")
        #
        gtable = cur.execute("select exists(select relname from pg_class where relname='seen_geonames')")
        gtable = cur.fetchone()[0]
        if not gtable:
            cur.execute("CREATE TABLE seen_geonames (id serial PRIMARY KEY, urlname varchar, geoname int);")
        #
        for loc_dict, tagcat_dictionary in read_locations(csv_file, conn, file_cfg):
            print ("READING LOCATION", loc_dict)
            count += 1
            url = 'http://api.metropublisher.com/' + loc_dict['urlname']				 
            namespace = uuid.NAMESPACE_URL
            url = url.encode('utf-8')
            new_url = '%s/%s/%s' % (file_cfg.API_KEY, str(file_cfg.INSTANCE_ID), url) 
            loc_uuid = uuid.uuid3(namespace, new_url)		# use also API_KEY and INSTANCE_ID - repeatable uuids x idempotent api 
            locupdater = LocationUpdater(api, file_cfg.INSTANCE_ID)	
            l_status = locupdater.upsert_location(loc_dict, loc_uuid)  
            # tags
            if file_cfg.has_tags == True:
                tagupdater = TagUpdater(api, file_cfg.INSTANCE_ID)	
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

We propose a csv template for which the script is ready-to-use, 'csv_template', in the examples folder 

external_unique_id is the field used to univocally identify the item in the database. 
If the client data are lacking it, we: 1) import locations and assign one external_unique_id, 2) export it back, 3) the client uses that for any update
"""

# TODO every client should be able to use easily our template if they create the csv that way

CSVFILES = [
        dict(
            client='MP-template',			
            uuid_import_id = 'http://client.com/import/0/',
            INSTANCE_ID = 0, # demo instance
            API_KEY = 'mxvsm129bm7RgcGRYedzLersZXGQSwQjMiyilovZL7A',
            API_SECRET = 'hSBADtfwcEnxeatj',
            GOOGLE_API_KEY = 'AIzaSyDrFNq9li1esIyHypfNh1IZ0w4FcPDeOVs',
            GEONAME_USER = 'kiarasky2015',  
            aux_database = 'aux_location_db',
            basepath = '/home/kiarasky/GIT/van_api/',
            csvfile = 'csv_template',
            csvfields = namedtuple( 'MPlocation' ,['uuid','id', 'urlname', 'published', 'title', 'phone', 'email', 'web','number', 'street', 'postalcode','city', 'fax', 'description', 'print_description', 'content', 'price', 'reservation_url', 'region', 'country','creation_date', 'image','thumbnail', 'video', 'facebook', 'twitter', 'tags_categories']), 
            external_unique_id = 'uuid' 			# they give us an unique uuid, this column says which one to use - if None, create it on-the-fly and re-export
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
            has_tags = True,
            basepath = '/',
            csvfile = 'their_file_name',
            csvfields = namedtuple( 'VAlocation' , ['cat_region', 'cat_category', 'title', 'street', 'city', 'postalcode', 'phone', 'web']),
            ),
           ]


CsvFile = namedtuple('CsvFile', ['client','uuid_import_id','INSTANCE_ID', 'API_KEY', 'API_SECRET', 'GOOGLE_API_KEY', 'GEONAME_USER', 'aux_database','decode','has_tags', 'basepath', 'csvfile', 'csvfields', 'external_unique_id'])
_default = CsvFile('required','required','required','required','required','required','required', 'required', 'UTF-8', False, 'required', 'required', None, None)
CSVFILES = [_default._replace(**t) for t in CSVFILES]
# TODO add error message if required fields missing!



def read_locations(csv_file, conn, file_cfg): 
    """This function reads the csv, and based on the configuration creates and yelds location dictionaries for the API
    """
    counter = 0
    tagcat_dictionary = {}
    with open(csv_file, 'r') as csvfile:
        next(csvfile) # skip header - add in cfg Has_header = True/False					
        reader = csv.reader(csvfile, delimiter=',')			
        LOCS = []
        for row in reader:
            print ("row", row)
            loc_dict = tagcat_dictionary = {} 					
            try:
                #row = decode_row(row, file_cfg.decode) # No need to decode anymore
                loc_dict, tagcat_dictionary = prepare_location_data(row, conn, file_cfg)
                counter += 1
                print ("yelding loc", loc_dict)
                yield loc_dict, tagcat_dictionary 
            except:
                print ('Failed on {}'.format(row))
                raise


def prepare_location_data(row, conn, file_cfg): 
    """This function gets a row and, based on the configuration, creates location dictionary for the API and tagcategory dictionary
    """
    # decide how to load the data, if use Namedtuple or if putting the columns somehow in the config
    # if fails, return None
    # TODO implement uuid option 
    loc_dict = tagcat_dict = namedtuple_row = None      
    if file_cfg.csvfields:	  		
        namedtuple_row = file_cfg.csvfields._make(row) 		# TODO use namedtuple? load each csv table on the correct namedtuple + removed encode_utf(row)
    else:
        print ("check conf file")
    if namedtuple_row:
        if file_cfg.client == "MP-template":
            print ("processing template data")
            loc_dict, tagcat_dict = get_template_location(namedtuple_row, conn, file_cfg)
        elif file_cfg.client == "VAliving":
            loc_dict, tagcat_dict = get_VAliving_location(namedtuple_row, conn, file_cfg)
        else:
            print ("implement specific function for client")
    else:
        print ("cound not create namedtuple")
    if loc_dict is not None and tagcat_dict is not None:
        return loc_dict, tagcat_dict         
    else:
        return None, None



def get_template_location(row, conn, file_cfg):
    loc_dict = tagcat_dictionary = {}
    loc_dict['title'] = row.title or None 
    if loc_dict['title'] is None: 
        raise NotImplementedError # skip
    if row.urlname:											# TODO generalize with unique_id_key or use uuid?
        urlname = row.urlname
    else:
        urlname = row.title.replace(' ','-').lower()				# we create the urlname, TODO use suggest_urlname, make sure it's uniq in the destination db 
        urlname_count = 0
        while urlname in LOCS:
            urlname_count += 1
            urlname = urlname + '-%s' % urlname_count
            print ('DUPLICATE URLNAME, TRYING', urlname)	
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
    if row.creation_date:
        loc_dict['created'] = loc_dict['modified'] = row.creation_date
    else:
        loc_dict['created'] = loc_dict['modified'] = str(datetime.datetime.now() - timedelta(days=2))	# yesterday's date
    if row.published: 
        if row.published == 1:
            loc_dict['state'] = 'published'
        else:
            loc_dict['state'] = 'draft'
    else:
        loc_dict['state'] = 'published' # TODO general, get status from csv												
    # 
    # coordinates use a pg db as filesystem to see if already retrieved!
    address_key = ''
    if row.street:
        address_key = address_key + row.street.replace(' ','+') + ','
    if row.city:
        address_key = address_key + row.city.replace(' ','+') + ','
    address_key = address_key.replace(' ', '+') 					
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
    else:
        coords = COORDS_CACHE.get(urlname)		
    if coords is None:	
        coords = get_coords(address_key, file_cfg.GOOGLE_API_KEY, loc_dict['urlname'])	
        loc_dict['coords'] = coords 	
        COORDS_CACHE[urlname] = coords
        cur.execute("INSERT INTO seen_coords(urlname, coords) VALUES (%s, %s)", (c_urlname, coords))
        conn.commit()
    else:
        loc_dict['coords'] = coords
    #
    # geonames - TODO for API add a warning for invalid geonames? Or set default to None?
    cur.execute("SELECT * FROM seen_geonames where urlname = (%s)", (c_urlname,))
    gresult = cur.fetchone()
    if gresult:
        geoname = gresult[2]
    else:
        search_title = loc_dict['title'].replace(' ', '+')
        geoname = GEONAMES_CACHE.get(urlname)				
        if geoname is None:
            gcity = row.city.replace(' ', '+')
            geoname = get_geoname(file_cfg.GEONAME_USER, loc_dict['pcode'], gcity)
        if geoname is None:
            print ("geoname from API is None")
        else:		 
            GEONAMES_CACHE[urlname] = geoname	  
            cur.execute("INSERT INTO seen_geonames(urlname, geoname) VALUES (%s, %s)", (c_urlname, geoname))
            conn.commit()
    if geoname:
        loc_dict['geoname_id'] = int(str(geoname))
    else:
        loc_dict['geoname_id'] = None
    return loc_dict, tagcat_dictionary 



def get_VAliving_location(row, conn, file_cfg):
    print ("to be implemented")
    return None, None




if __name__ == '__main__':
    sys.exit(main())


