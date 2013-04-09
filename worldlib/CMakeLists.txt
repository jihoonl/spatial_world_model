cmake_minimum_required(VERSION 2.8.3)
project(worldlib)

## Find catkin macros and libraries
## if COMPONENTS list like find_package(catkin REQUIRED COMPONENTS xyz)
## is used, also find other catkin packages
find_package(catkin REQUIRED COMPONENTS rospy world_msgs rospy_message_converter actionlib geometry_msgs)

## Uncomment this if the package has a setup.py. This macro ensures
## modules and scripts declared therein get installed
catkin_python_setup()

#######################################
## Declare ROS messages and services ##
#######################################

## Generate actions in the 'action' folder
add_action_files(DIRECTORY action
  FILES
  CreateWorldObjectDescription.action
  CreateWorldObjectInstance.action
  RemoveWorldObjectInstance.action
  GetWorldObjectDescription.action
  UpdateWorldObjectInstance.action
  WorldObjectInstanceTagSearch.action
  WorldObjectDescriptionTagSearch.action
)

generate_messages(
  DEPENDENCIES
  actionlib_msgs
  world_msgs
)

## LIBRARIES: libraries you create in this project that dependent projects also need
## CATKIN_DEPENDS: catkin_packages dependent projects also need
## DEPENDS: system dependencies of this project that dependent projects also need
catkin_package(
  CATKIN_DEPENDS actionlib_msgs
)

#############
## Install ##
#############

install(PROGRAMS scripts/spatial_world_model scripts/setup_world_model
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)
