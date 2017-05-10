#!/bin/bash -xe

# Source:
#   https://github.com/ndeepesh/taxi-usage-comparison-bigdata
# Run this script as a bootstrap action in EMR. This will install all the
# python packages that are required by the map-reduce scripts
sudo yum-config-manager --enable epel
sudo yum install -y spatialindex spatialindex-devel
sudo ln -sf /usr/bin/python2.7 /usr/bin/python
sudo yum install -y geos geos-devel
sudo yum install -y tmux
yes | sudo pip install shapely
yes | sudo pip install Rtree
yes | sudo pip install networkx
