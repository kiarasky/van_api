#!/usr/bin/python
"""Import Locations from a CSV file to mp3.
"""

import os
import sys
import csv
import json
import cgi
from uuid import uuid5, NAMESPACE_URL
import logging
import datetime
import ast
import sqlite3
#import psycopg2
from datetime import timedelta
from collections import namedtuple

import van_api

sys.path.append('/home/kiarasky/GIT/mp.importer/')
from mp.importer.csv import LocationUpdater, TagUpdater
from mp.importer.csv import get_coords, get_geoname, decode_row, encode_utf
#from mp.importer.urlname import suggest as suggest_urlname # gives error icu.InvalidArgsError: (<class 'icu.Transliterator'>, 'createInstance', ('Any-Latin; Latin ASCII',))


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')			
    for file_cfg in CSVFILES: #  TODO client should be an argument, no need to parse through csvfiles
        credentials = van_api.ClientCredentialsGrant(file_cfg.API_KEY, file_cfg.API_SECRET)
        api = van_api.API('api.metropublisher.com', credentials)
        if os.path.exists(file_cfg.logfile):            
            os.remove(file_cfg.logfile)
        logfile = open(file_cfg.logfile, 'a')     # open here, pass by and closes automatically at the end
        logfilewriter = csv.writer(logfile) 
        context = Context ( 	
                       problem_csv = logfilewriter, 
                       COORDS_CACHE = [],
                       GEONAMES_CACHE = [],
                       LOCS = [],
                       api = api,
                       problem_summary = {}
                      )	
        csv_file = file_cfg.basepath + file_cfg.csvfile		
        count = ok = skip = tags = 0 
        try:
            # sqlite database - "cache" is in the current folder - needs explicit commit
            conn = sqlite3.connect(file_cfg.aux_database)
        except:
            context.prob('error',"I am unable to connect to the database",None, None)
            break 																					
        cur = conn.cursor()
        cur.execute("create table if not exists seen_coords (id serial PRIMARY KEY, urlname varchar, coords varchar)")
        cur.execute("create table if not exists seen_geonames (id serial PRIMARY KEY, urlname varchar, geoname int)")
        #
        for loc_dict, tagcat_dictionary in read_locations(context, csv_file, conn, file_cfg):
            print ("READING LOCATION", loc_dict)
            count += 1
            locupdater = LocationUpdater(api, file_cfg.INSTANCE_ID)	
            l_status = locupdater.upsert_location(loc_dict, loc_dict['uuid'])  
            # tags
            if file_cfg.has_tags == True and tagcat_dictionary is not None and not tagcat_dictionary == {}:
                tagupdater = TagUpdater(api, file_cfg.INSTANCE_ID)	
                t_status = tagupdater.upsert_tags(tagcat_dictionary, loc_uuid)  
                if t_status == 1:
                    tags += 1
            if l_status == 1:
                ok += 1
            else:
                skip += 1    
        # commit
        conn.commit()
        # close db connection
        cur.close()
        conn.close()
        logging.info('Imported {} and removed {} out of {} locations'.format(ok, skip, count))
        logging.info('Imported also {} tags'.format(tags))
        return 0					     



_Context = namedtuple('Context', 'problem_csv COORDS_CACHE GEONAMES_CACHE LOCS api problem_summary')

Problem = namedtuple('Problem', """
        type
        msg
        uuid""")

class Context(_Context):
    def prob(self, type, msg=None, moreinfo=None, uuid=None):                            	
        context = self
        assert type in ['info', 'fix', 'fuzzy_fix', 'error']
        logging.debug('PROBLEM (%s)\n%s', type, msg)  			 
        if self.problem_csv is not None:
            if msg is not None:
                msg = msg.encode('utf-8')
            self.problem_csv.writerow([ 				  
                type,
                msg,
                moreinfo,
                uuid
                ])
        s = context.problem_summary.setdefault(type, {})  
        s[msg] = s.get(msg, 0) + 1




"""CONFIGURATION SETUP
Each client has a configuration setup including credentials

We propose a csv template for which the script is ready-to-use, 'csv_template', in the examples folder 
If clients have the table like that they can import without touching the code

external_unique_id is the field used to univocally identify the item in the database. 
If the client data are lacking it, we: 1) import locations and assign one external_unique_id, 2) export it back, 3) the client uses that for any update
"""

CSVFILES = [
        dict(
            client='MP-template',			
            INSTANCE_ID = 1, 
            # user for bugfix (id=36),
            #API_KEY = 'sosdxwfKCAh0E_Tjt36pcsYooYZhXXzdI9YYqZ-k',
            #API_SECRET = 'kd_YEcqkmt75ST5IWIzX-Qhgy4ss0l7aj1KQVpS6',
            # user for demo (id=1)
            API_KEY = 'mxvsm129bm7RgcGRYedzLersZXGQSwQjMiyilovZL7A',
            API_SECRET = 'hSBADtfwcEnxeatj',
            GOOGLE_API_KEY = 'AIzaSyDrFNq9li1esIyHypfNh1IZ0w4FcPDeOVs',
            GEONAME_USER = 'kiarasky2015',  
            aux_database = 'aux_location_db',
            basepath = '/home/kiarasky/GIT/van_api/',
            csvfile = 'csv_template',
            csvfields = namedtuple( 'MPlocation' ,['uuid','id', 'urlname', 'published', 'title', 'phone', 'email', 'web','number', 'street', 'postalcode','city', 'fax', 'description', 'print_description', 'content', 'price', 'reservation_url', 'region', 'country','creation_date', 'image','thumbnail', 'video', 'facebook', 'twitter', 'tags_categories']), 
            # tags_categories of type: '{"tag1":"cat1","tag2":None,"tag3":"cat1,cat2"}'
            external_id = 'uuid', 
            Hasheader = True,
            logfile = 'logfile.csv', 			
            ),
        dict(
            client='VALiving',
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


CsvFile = namedtuple('CsvFile', ['client','INSTANCE_ID', 'API_KEY', 'API_SECRET', 'GOOGLE_API_KEY', 'GEONAME_USER', 'aux_database','decode','has_tags', 'basepath', 'csvfile', 'csvfields', 'external_id', 'Hasheader', 'logfile'])
_default = CsvFile('required','required','required','required','required','required', 'required', 'UTF-8', False, 'required', 'required', None, None, False, 'logfile.csv')
CSVFILES = [_default._replace(**t) for t in CSVFILES]
# TODO add error message if required fields missing!



def read_locations(context, csv_file, conn, file_cfg): 
    """This function reads the csv, and based on the configuration creates and yelds location dictionaries for the API
    """
    counter = 0
    tagcat_dictionary = {}
    with open(csv_file, 'r') as csvfile:
        if file_cfg.Hasheader == True:
            next(csvfile) 					
        reader = csv.reader(csvfile, delimiter=',')			
        for row in reader:
            print ("row", row)
            loc_dict = tagcat_dictionary = {} 					
            try:
                loc_dict, tagcat_dictionary = prepare_location_data(context, row, conn, file_cfg)
                counter += 1
                print ("yelding loc", loc_dict)
                if counter >= 2:
                    break
                yield loc_dict, tagcat_dictionary 
            except:
                print ('Failed on {}'.format(row))
                context.prob('error','Failed to read this row',row,None)
                raise


def prepare_location_data(context, row, conn, file_cfg): 
    """This function gets a row and, based on the configuration, creates location dictionary for the API and tagcategory dictionary
    """
    # TODO decide how to load the data, if use Namedtuple or if putting the columns somehow in the config
    loc_dict = tagcat_dict = namedtuple_row = None      
    if file_cfg.csvfields:	  		
        namedtuple_row = file_cfg.csvfields._make(row) 		
    else:
        print ("check conf file")
    if namedtuple_row:
        if file_cfg.client == "MP-template":
            print ("processing template data")
            loc_dict, tagcat_dict = get_template_location(context, namedtuple_row, conn, file_cfg)
        elif file_cfg.client == "VAliving":
            loc_dict, tagcat_dict = get_VAliving_location(context, namedtuple_row, conn, file_cfg)
        else:
            print ("implement specific function for client")
    else:
        print ("cound not create namedtuple")
    if loc_dict is not None and tagcat_dict is not None:
        return loc_dict, tagcat_dict         
    else:
        return None, None


def create_uuid(row, file_cfg, attribute):   
    external_id_value = getattr(row,attribute)
    assert external_id_value is not None
    ns = '%s/%s/%s' % (file_cfg.client, file_cfg.API_KEY, external_id_value)
    uuid = uuid5(NAMESPACE_URL, ns)  
    return uuid


def get_template_location(context, row, conn, file_cfg):
    loc_dict = tagcat_dictionary = {}
    # urlname 
    if row.urlname:											
        urlname = row.urlname
    else:
        urlname = row.title.replace(' ','-').lower()		# use suggest.urlname
    locupdater = LocationUpdater(context.api, file_cfg.INSTANCE_ID)	
    found = locupdater.check_existing_location(urlname) 
    # *********************************************************** TODO while and create an unique urlname
    if found:
        urlname_count = 0
        while urlname in context.LOCS:
            urlname_count += 1
            urlname = urlname + '-%s' % urlname_count
            print ('DUPLICATE URLNAME, TRYING', urlname)
        context.LOCS.append(urlname)
        loc_dict['urlname'] = urlname
    else:
        loc_dict['urlname'] = urlname    
    assert 'urlname' in loc_dict  
    #
    # uuid
    if file_cfg.external_id:
        loc_dict['uuid'] = create_uuid(row, file_cfg, file_cfg.external_id)
    else: # use urlname recently created, unique
        loc_dict['uuid'] = create_uuid(row, file_cfg, loc_dict['urlname'])
    assert loc_dict['uuid'] is not None
    #       
    loc_dict['title'] = row.title or None 
    if loc_dict['title'] is None: 
        context.prob('error','Skipped location with no title',None,loc_dict['uuid'])
        raise NotImplementedError								
    #
    loc_dict['street'] = row.street or None
    loc_dict['pcode'] = row.postalcode or None
    loc_dict['phone'] = row.phone or None
    #
    # create tagcat_dictionary {"tag1":"cat1","tag2":None,"tag3":"cat1,cat2"}
    if row.tags_categories and row.tags_categories.strip():
        tagcat_dictionary = dict(ast.literal_eval(row.tags_categories))   
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
        loc_dict['state'] = 'published' 											
    # 
    # coords (address_key specific of each csvfile)
    address_key = ''
    if row.street:
        address_key = address_key + row.street.replace(' ','+') + ','
    if row.city:
        address_key = address_key + row.city.replace(' ','+') + ','
    address_key = address_key.replace(' ', '+') 	
    coords = get_location_coordinates(context, address_key, conn, file_cfg, loc_dict['urlname'])
    if coords:
        loc_dict['coords'] = coords
    # geonames
    geoname = get_location_geoname(context, row, conn, file_cfg, loc_dict['urlname'])
    if geoname: # can also be None
        loc_dict['geoname_id'] = int(str(geoname)) 
    return loc_dict, tagcat_dictionary 




### these two can be used for all csvfiles - could be added to csv.py but maybe better keep these here (so csv.py does not rely on sqlite)

def get_location_coordinates(context, address_key, conn, file_cfg, urlname):
    cur = conn.cursor()
    cur.execute("SELECT * FROM seen_coords where urlname = (?)", (urlname,))  # sqlite uses ? as placeholder
    result = cur.fetchone()
    if result and result[2]:						# if it's None, i can re-try, but it will give none again - TODO log errors
        coords = []
        for i in result[2].split(','):
            i = i.replace('"','')
            i = i.replace('{','')
            i = i.replace('}','')
            coords.append(float(i))
    else:
        coords = COORDS_CACHE.get(urlname)		
    if coords is None:	
        coords = get_coords(address_key, file_cfg.GOOGLE_API_KEY, urlname)	
        if coords is None:
            context.prob('error','No coords for this location',None,loc_dict['uuid'])
        loc_dict['coords'] = coords 	
        COORDS_CACHE[urlname] = coords
        cur.execute("INSERT INTO seen_coords(urlname, coords) VALUES (?,?)", (urlname, coords))
        conn.commit()
    return coords


def get_location_geoname(context, row, conn, file_cfg, urlname):
    cur.execute("SELECT * FROM seen_geonames where urlname = (?)", (urlname,))
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
            context.prob('error','No geoname for this location',None,loc_dict['uuid'])
        else:		 
            GEONAMES_CACHE[urlname] = geoname	  
            cur.execute("INSERT INTO seen_geonames(urlname, geoname) VALUES (?,?)", (curlname, geoname))
            conn.commit()
    return geoname



def get_VAliving_location(context, row, conn, file_cfg):
    print ("to be implemented")
    return None, None




if __name__ == '__main__':
    sys.exit(main())


