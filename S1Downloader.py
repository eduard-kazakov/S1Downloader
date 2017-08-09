# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 12:49:33 2017

@author: silent
"""

import urllib2, urllib
import os
from datetime import datetime
import xml.etree.ElementTree as etree

class S1Downloader():
    
    opensearch_base_url =  'https://scihub.copernicus.eu/dhus/' 
    odata_base_url = 'https://scihub.copernicus.eu/dhus/odata/v1/'
    
    sensor_modes= ['SW','IW','EW','WV']
    product_types = ['SLC','GRD','OCN']
    polarisations = ['HH','HV','VH','VV','HH+HV','VV+VH']
    
    def __init__(self, username, password, download_dir):
        self.username = username
        self.password = password
        self.download_dir = download_dir
        
        self.__connect()
        
        
    def __connect(self):
        self.password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        self.password_mgr.add_password(None, self.opensearch_base_url, self.username, self.password)
        self.handler = urllib2.HTTPBasicAuthHandler(self.password_mgr)  
        self.opener = urllib2.build_opener(self.handler)  
        urllib2.install_opener(self.opener)

    def search_by_conditions (self, wkt_region=None, start_date=None, end_date=None, sensor_modes=None, polarisations=None, product_types=None, extra=None):
        rows = 100
        
        search_string = self.opensearch_base_url + 'search?q='
        conditions = []
        if wkt_region:
            conditions.append('footprint:"Intersects(%s)"' % wkt_region)
        if start_date and not end_date:
            conditions.append('beginposition:[%s TO NOW]' % self.__datetime_to_scihub_format(start_date))
        if start_date and end_date:
            conditions.append('beginposition:[%s TO %s]' % (self.__datetime_to_scihub_format(start_date),self.__datetime_to_scihub_format(end_date)))
            conditions.append('endposition:[%s TO %s]' % (self.__datetime_to_scihub_format(start_date),self.__datetime_to_scihub_format(end_date)))
        if end_date and not start_date:
            conditions.append('endposition:[%s TO %s]' % (self.__datetime_to_scihub_format(datetime(year=1970,month=1,day=1)),self.__datetime_to_scihub_format(end_date)))
        if sensor_modes:
            sensor_conditions = []
            for sensor_mode in sensor_modes:
                if not sensor_mode in self.sensor_modes:
                    print 'Invalid sensor mode'
                    return []
                else:
                    if len(sensor_modes) == 1:
                        conditions.append(sensor_mode)
                    else:
                        sensor_conditions.append(sensor_mode)
            if sensor_conditions:
                conditions.append('(' + ' OR '.join(sensor_conditions) + ')')
                        
        if polarisations:
            polarisation_conditions = []
            for polarisation in polarisations:
                if not polarisation in self.polarisations:
                    print 'Invalid polarisation'
                    return []
                else:
                    if len(polarisations) == 1:
                        conditions.append(polarisation)
                    else:
                        polarisation_conditions.append(polarisation)
            if polarisation_conditions:
                conditions.append('(' + ' OR '.join(polarisation_conditions) + ')')

        if product_types:
            product_conditions = []
            for product_type in product_types:
                if not product_type in self.product_types:
                    print 'Invalid product type'
                    return []
                else:
                    if len(product_types) == 1:
                        conditions.append(product_type)
                    else:
                        product_conditions.append(product_type)
            if product_conditions:
                conditions.append('(' + ' OR '.join(product_conditions) + ')')
        
        # service
        conditions.append('S1')
        
        
        if conditions:
            conditions = ' AND '.join(conditions)
        else:
            print 'No conditions specified!'
            return []
        
        base_search_string = search_string + conditions
        print base_search_string
        base_search_string = base_search_string + '&rows=%s' % str(rows)

        
        # First step: get number of results:
        datasource = urllib.urlopen(base_search_string)
        number_of_results = int(etree.parse(datasource).getroot().find('{http://a9.com/-/spec/opensearch/1.1/}totalResults').text)
        print number_of_results, ' results'
        
        searched_data = []
            
        if number_of_results <= rows:
            answer_data = etree.parse(datasource).getroot()
            entries = answer_data.findall('{http://www.w3.org/2005/Atom}entry')
            for entry in entries:
                entry_properties = self.__get_entry_properties(entry) 
                searched_data.append(entry_properties)
            
        else:
            for start_row in range(0,number_of_results,rows):
                current_search_string = base_search_string + '&start=%s' % str(start_row)
                print current_search_string
                datasource = urllib.urlopen(current_search_string)
                answer_data = etree.parse(datasource).getroot()
                entries = answer_data.findall('{http://www.w3.org/2005/Atom}entry')
                for entry in entries:
                    entry_properties = self.__get_entry_properties(entry) 
                    searched_data.append(entry_properties)
                
        return searched_data
        
    def download_by_conditions (self, wkt_region=None, start_date=None, end_date=None, sensor_modes=None, polarisations=None, product_types=None, extra=None):
        search_results = self.search_by_conditions(wkt_region=wkt_region,
                                                   start_date=start_date,
                                                   end_date=end_date,
                                                   sensor_modes=sensor_modes,
                                                   polarisations=polarisations,
                                                   product_types=product_types,
                                                   extra=extra)
        
        for search_result in search_results:
            print 'Downloading %s (size:%s)' % (search_result['name'], search_result['size'])
            current_path = self.download_dir + search_result['name'] + '.' + search_result['data_format']
            self.download_scene_by_id (search_result['uid'], current_path)
                

    def download_by_meta4_list (self, meta4_list):
        tree = etree.parse(meta4_list)
        doc = tree.getroot()
        for child in doc:
            fileName = child.attrib['name'] #child.attrib.values()
            sentinel_link = child[-1].text
            print 'fileName is ', fileName, 'link is ', sentinel_link
            destinationpath =  self.download_dir + fileName  
        
            if os.path.exists(destinationpath):  
                    print fileName, ' already downloaded'  
                    continue  
      
            print "Downloading ", fileName 
            urllib.urlretrieve(sentinel_link, destinationpath)    
 
            print "Done downloading"
        
        
    def download_scene_by_id (self, uid, downloading_path):
        downloader_string = self.odata_base_url + 'Products(\'$s\')/$value' % str(uid)
        print 'Downloading %s' % str(id)
        urllib.urlretrieve(downloader_string,downloading_path)
        
    def __datetime_to_scihub_format(self, user_datetime):
        return user_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    def __get_entry_properties(self, entry):
        product_type = ''
        polarisation = ''
        start_date = ''
        end_date = ''
        size = ''
        uuid = ''
        name = ''
        
        for child in entry:
            if child.get('name') == 'producttype':
                product_type = child.text
            if child.get('name') == 'polarisationmode':
                polarisation = child.text
            if child.get('name') == 'beginposition':
                start_date = child.text
            if child.get('name') == 'endposition':
                end_date = child.text
            if child.get('name') == 'size':
                size = child.text
            if child.get('name') == 'uuid':
                uuid = child.text
            if child.get('name') == 'identifier':
                name = child.text
            if child.get('name') == 'data_format':
                data_format = child.text
        return {'product_type':product_type, 'polarisation':polarisation, 'start_date':start_date,
                'end_date':end_date, 'size':size, 'uid':uuid, 'name':name, 'data_format':data_format}
