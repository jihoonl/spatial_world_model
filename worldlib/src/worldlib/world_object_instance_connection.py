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
The WorldObjectInstanceConnection class provides functions to natively communicate with a PostgreSQL 
World Model database for the world_object_instances table. World object instances are particular
instances of an object and link to an external description. This can be thought of as the robot's
working memory.

@author:  Russell Toris
@version: February 18, 2013
'''

import psycopg2
import thread

class WorldObjectInstanceConnection(object):
    '''
    The main WorldObjectInstanceConnection object which communicates with the PostgreSQL World Model 
    database.
    '''

    def __init__(self, user, pwd, host='localhost'):
        '''
        Creates the WorldObjectInstanceDatabase object and connects to the world object instance
        database.
        
        @param user: the database username
        @type  user: string
        @param pwd: the database password
        @type  pwd: string
        @param host: the database hostname
        @type  host: string
        '''
        # fields in the database that are timestamps
        self.timestamps = ['creation', 'update', 'perceived_end', 'pose_stamp']
        # name of the world object instances table
        self._woi = 'world_object_instances'
        # name of the database
        self._db = 'world_model'
        # connect to the world model database
        self.conn = psycopg2.connect(database=self._db, user=user, password=pwd, host=host)
        # create a lock 
        self.lock = thread.allocate_lock()

    def insert(self, entity):
        '''
        Insert the given entity into the world_object_instances table. This will create a new 
        instance. The instance_id will be set to a unique value and returned.
        
        @param entity: the entity to insert with the correct keys for the columns
        @type  entity: dict
        @return: the instance_id
        @rtype: integer
        '''
        # ensure the instance ID does not get set by the user
        if 'instance_id' in entity.keys():
            del entity['instance_id']
        # build the SQL
        helper = self._build_sql_helper(entity)
        with self.lock:
            # create a cursor
            cur = self.conn.cursor()
            # build the SQL
            cur.execute("""INSERT INTO """ + self._woi + 
                        """ (instance_id, """ + helper['cols'] + """) 
                        VALUES (nextval('world_object_instances_instance_id_seq'), 
                        """ + helper['holders'] + """) RETURNING instance_id""", helper['values'])
            instance_id = cur.fetchone()[0]
            self.conn.commit()
            cur.close()
        # return the instance ID
        return instance_id

    def delete(self, instance_id): 
        '''
        Delete the entity of the given instance_id from world_object_instances table.

        @param instance_id : the unique identifier of entity
        @type  instance_id : integer
        @return: result
        @rtype: bool 
        '''
        with self.lock:
            # create a cursor
            cur = self.conn.cursor()
            # build the SQL
            cur.execute("""DELETE FROM """ + self._woi + """ WHERE instance_id = %s""",(instance_id,))
            self.conn.commit()
            cur.close()
            result = True
        return result

            
    def update_entity_by_instance_id(self, instance_id, entity):
        '''
        Update the entity in the world_object_instances table with the given instance_id, if one
        exists.
        
        @param instance_id: the instance_id of the entity to update
        @type  instance_id: int
        @param entity: the entity to update with
        @type  entity: dict
        @return: if an entity was found and updated with the given instance_id
        @rtype:  bool
        '''
        with self.lock:
            # create a cursor
            cur = self.conn.cursor()
            # check if the instance actually exists
            cur.execute("""SELECT instance_id FROM """ + self._woi + 
                        """ WHERE instance_id = %s""", (instance_id,))
            if len(cur.fetchall()) is 0:
                cur.close()
                result = False
            else:
                # ensure the instance ID does not get set by the user
                if 'instance_id' in entity.keys():
                    del entity['instance_id']
                # build the SQL
                helper = self._build_sql_helper(entity)
                # build the SQL
                helper['values'] += (instance_id,)
                cur.execute("""UPDATE """ + self._woi + 
                            """ SET (""" + helper['cols'] + """) = (""" + helper['holders'] + 
                            """) WHERE instance_id = %s""", helper['values'])
                self.conn.commit()
                cur.close()
                result = True
        return result
    
    def search_tags(self, tags):
        '''
        Search for and return all entities in the world_object_instances table that contain the
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
            sql = """SELECT * FROM """ + self._woi + """ WHERE ("""
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
    
    def _db_to_dict(self, entity):
        '''
        Convert a database tuple to a dict. This will also convert timestamps back into unix time.
        This function assumes the tuple is in the correct order.
        
        @param entity: the entity to build the dictionary for
        @type  entity: tuple
        @return: the dictionary containing the information from the database
        @rtype: dict
        '''
        # convert each one assuming the ordering is correct
        final = {
                'instance_id' : entity[0],
                'name' : entity[1],
                'creation' : None if entity[2] is None else float(entity[2].strftime('%s.%f')),
                'update' : None if entity[3] is None else float(entity[3].strftime('%s.%f')),
                'expected_ttl' : entity[4],
                'perceived_end' : None if entity[5] is None else float(entity[5].strftime('%s.%f')),
                'source_origin' : entity[6],
                'source_creator' : entity[7],
                'pose_seq' : entity[8],
                'pose_stamp' : None if entity[9] is None else float(entity[9].strftime('%s.%f')),
                'pose_frame_id' : entity[10],
                'pose_position' : entity[11],
                'pose_orientation' : entity[12],
                'pose_covariance' : entity[13],
                'description_id' : entity[14],
                'properties' : entity[15],
                'tags' : entity[16],
                }
        return final

    def _build_sql_helper(self, entity):
        '''
        A helper function to build the SQL for an insertion/update. This will take the entity dict
        and create a new dict containing a string of comma separated column names, a string of
        comma separated place holders (either '%s' or 'to_timestamp(...)'), and a tuple of the
        values.
        
        @param entity: the entity to build the SQL helper for
        @type  entity: dict
        @return: the dictionary containing the three helper variables
        @rtype: dict
        '''
        final = {'cols' : '', 'holders' : '', 'values' : ()}
        for k in entity.keys():
            final['cols'] += k + ', '
            # check if this is a timestamp
            if k in self.timestamps:
                final['holders'] += 'to_timestamp(' + str(entity[k]) + '), '
            else :
                final['holders'] += '%s, '
                final['values'] += (entity[k],)
        # remove trailing ', '
        final['cols'] = final['cols'][:-2]
        final['holders'] = final['holders'][:-2]
        return final
