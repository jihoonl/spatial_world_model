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
The WorldObjectDescriptionConnection class provides functions to natively communicate with a 
PostgreSQL world model database for the world_object_descriptions table. The world object
description is referenced by one or many world object instances are linked to one or many
individual descriptors.

@author:  Russell Toris
@version: February 18, 2013
'''

import psycopg2
import thread

class WorldObjectDescriptionConnection(object):
    '''
    The main WorldObjectDescriptionConnection object which communicates with the PostgreSQL world 
    model database.
    '''
    
    def __init__(self, user, pwd, host='localhost'):
        '''
        Creates the WorldObjectDescriptionDatabase object and connects to the world object
        description database.
        '''
        # name of the world object descriptions table
        self._wod = 'world_object_descriptions'
        # connect to the world model database
        self.conn = psycopg2.connect(database='world_model', user=user, password=pwd, host=host)
        # create a lock 
        self.lock = thread.allocate_lock()
        
    def insert(self, entity):
        '''
        Insert the given entity into the world object description database. This will create a new 
        description. The description_id will be set to a unique value and returned. Since
        descriptions are likely to be large, they will be saved to the gridfs.
        
        @param entity: the entity to insert
        @type  entity: dict
        @return: the description_id
        @rtype: string
        '''
        # ensure the description ID does not get set by the user
        if 'description_id' in entity.keys():
            del entity['description_id']
        # build the SQL
        helper = self._build_sql_helper(entity)
        with self.lock:
            # create a cursor
            cur = self.conn.cursor()
            # build the SQL
            cur.execute("""INSERT INTO """ + self._wod + 
                        """ (description_id, """ + helper['cols'] + """) 
                        VALUES (nextval('world_object_descriptions_description_id_seq'), 
                        """ + helper['holders'] + """) RETURNING description_id""", helper['values'])
            description_id = cur.fetchone()[0]
            self.conn.commit()
            cur.close()
        # return the description ID
        return description_id

    def search_description_id(self, description_id):
        '''
        Search for and return the entity in the world_object_descriptions table with the given 
        description_id, if one exists.
        
        @param description_id: the description_id field of the entity to search for
        @type  description_id: int
        @return: the entity found, or None if an invalid description_id was given
        @rtype:  dict
        '''
        with self.lock:
            # create a cursor
            cur = self.conn.cursor()
            # check if the description actually exists
            cur.execute("""SELECT * FROM """ + self._wod + 
                        """ WHERE description_id = %s""", (description_id,))
            result = cur.fetchone()
            cur.close()
        if result is None:
            return None
        else:
            return self._db_to_dict(result)
        
    def search_tags(self, tags):
        '''
        Search for and return all entities in the world_object_descriptions table that contain the
        given list of tags.
        
        @param tags: the list of tags to search for
        @type  tags: list
        @return: the entities found
        @rtype: list
        '''
        final = []
        # do not search empty arrays
        if len(tags) > 0:
            # build the SQL
            sql = """SELECT * FROM """ + self._wod + """ WHERE ("""
            values = ()
            for t in tags:
                sql += "%s = ANY (tags) AND "
                values += (t,)
            # remove the trailing ' AND '
            sql = sql[:-5] + """);"""
            with self.lock:
                # create a cursor
                cur = self.conn.cursor()
                cur.execute(sql, values)
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
        
        @param entity: the entity to build the dictionary for
        @type  entity: tuple
        @return: the dictionary containing the information from the database
        @rtype: dict
        '''
        # convert each one assuming the ordering is correct
        final = {
                'description_id' : entity[0],
                'name' : entity[1],
                'tags' : entity[2],
                }
        return final