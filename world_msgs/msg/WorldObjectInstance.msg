# a unique identifier for this instance
int32 instance_id
# a human-readable name for this instance
string name
# creation time of the instance
time creation
# last time this instance was updated
time update
# an estimate of how long this instance's information will be valid
duration expected_ttl
# the actual time this instance was removed from the robot's working memory
time perceived_end
# source information for the origin of this instance
world_msgs/Source source
# position information with a belief state
geometry_msgs/PoseWithCovarianceStamped pose
# spatial descriptions of the object itself
int32 description_id
# object properties and relationships
string[] properties
# high level tags
string[] tags