# Software License Agreement (BSD License)
#
# Copyright (c) 2013, Willow Garage, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following
# disclaimer in the documentation and/or other materials provided
# with the distribution.
# * Neither the name of Willow Garage, Inc. nor the names of its
# contributors may be used to endorse or promote products derived
# from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

'''
The DescriptorConnection class provides functions to natively communicate with a PostgreSQL 
World Model database for the descriptors table. A descriptor is linked to a single world object
description.

@author:  Russell Toris
@version: February 18, 2013
'''

import psycopg2
import thread

class DescriptorConnection(object):
    '''
    The main DescriptorConnection object which communicates with the PostgreSQL World Model 
    database.
    '''

    def __init__(self, user, pwd, host='localhost'):
        '''
        Creates the DescriptorConnection object and connects to the descriptors table.
        
        @param user: the database username
        @type  user: string
        @param pwd: the database password
        @type  pwd: string
        @param host: the database hostname
        @type  host: string
        '''
        # name of the descriptors table
        self._descriptors = 'descriptors'
        # name of the database
        self._db = 'world_model'
        # connect to the world model database
        self.conn = psycopg2.connect(database=self._db, user=user, password=pwd, host=host)
        # create a lock 
        self.lock = thread.allocate_lock()

    def insert(self, entity):
        '''
        Insert the given entity into the descriptors table. This will create a new descriptor. The 
        descriptor_id will be set to a unique value and returned. Note that any data found in the 
        data field (if any) will be stored into a Large Object and the OID will be placed in the 
        spot of their value.
        
        @param entity: the entity to insert with the correct keys for the columns
        @type  entity: dict
        @return: the descriptor_id
        @rtype: integer
        '''
        # ensure the descriptor ID does not get set by the user
        if 'descriptor_id' in entity.keys():
            del entity['descriptor_id']
        with self.lock:
            # check if there is data
            if 'data' in entity.keys():
                # store the data in a Large Object
                lobj = self.conn.lobject()
                lobj.write(entity['data'])
                self.conn.commit()
                entity['data'] = lobj.oid
            # build the SQL
            helper = self._build_sql_helper(entity)
            # create a cursor
            cur = self.conn.cursor()
            cur.execute("""INSERT INTO """ + self._descriptors + 
                        """ (descriptor_id, """ + helper['cols'] + """) 
                        VALUES (nextval('descriptors_descriptor_id_seq'), 
                        """ + helper['holders'] + """) RETURNING descriptor_id""", helper['values'])
            descriptor_id = cur.fetchone()[0]
            self.conn.commit()
            cur.close()
        # return the descriptor ID
        return descriptor_id
    
    def search_by_description_id(self, description_id):
        '''
        Search for and return all entities in the descriptors table with the given description_id, 
        if any. This will load the file and return the contents.
        
        @param description_id: the description_id to search for
        @type  description_id: int
        @return: the entities found
        @rtype:  list
        '''
        final = []
        with self.lock:
            # create a cursor
            cur = self.conn.cursor()
            # check if the description actually exists
            cur.execute("""SELECT * FROM """ + self._descriptors + 
                        """ WHERE description_id = %s""", (description_id,))
            # extract the values
            results = cur.fetchall()
            for r in results:
                # convert to a dictionary and convert the timestamps
                final.append(self._db_to_dict(r))
            cur.close()
        return final
    
    def _build_sql_helper(self, entity):
        '''
        A helper function to build the SQL for an insertion/update. This will take the entity dict
        and create a new dict containing a string of comma separated column names, a string of
        comma separated place holders (e.g., '%s'), and a tuple of the values.
        
        @param entity: the entity to build the SQL helper for
        @type  entity: dict
        @return: the dictionary containing the three helper variables
        @rtype: dict
        '''
        final = {'cols' : '', 'holders' : '', 'values' : ()}
        for k in entity.keys():
            final['cols'] += k + ', '
            final['holders'] += '%s, '
            final['values'] += (entity[k],)
        # remove trailing ', '
        final['cols'] = final['cols'][:-2]
        final['holders'] = final['holders'][:-2]
        return final

    def _db_to_dict(self, entity):
        '''
        Convert a database tuple to a dict. This function assumes the tuple is in the correct order.
        This function will load the data in the data field.
        
        @param entity: the entity to build the dictionary for
        @type  entity: tuple
        @return: the dictionary containing the information from the database
        @rtype: dict
        '''
        # load the data
        if entity[3] is not None:
            lobj = self.conn.lobject(entity[3])
            data = lobj.read()
        else:
            data = None
        # convert each one assuming the ordering is correct
        final = {
                'descriptor_id' : entity[0],
                'description_id' : entity[1],
                'type' : entity[2],
                'data' : data,
                'ref' : entity[4],
                'tags' : entity[5],
                }
        return final