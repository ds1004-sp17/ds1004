from __future__ import print_function

import csv
import re
import argparse
import os
from cStringIO import StringIO
import json
from shapely.geometry import mapping, shape
from pyspark import SparkContext, SparkConf
import itertools
import networkx as nx

threshold = 0.5

parser = argparse.ArgumentParser(description='Join Taxi and Zillow shapefiles')
parser.add_argument('--K', type=int, default=5, help='neighbors to consider.')
parser.add_argument('--taxi_shapes_path', type=str,
        default='taxi_zones.geojson',
        help='taxi shapefile.')
parser.add_argument('--taxi_edges_path', type=str,
        default='taxi_zones_LocationID_edges.json',
        help='taxi regions adjacency.')
parser.add_argument('--zillow_shapes_path', type=str,
        default='ZillowNeighborhoods-NY.geojson',
        help='zillow shapefile.')
parser.add_argument('--zillow_edges_path', type=str,
        default='ZillowNeighborhoods_RegionID_edges.json',
        help='Zillow neighborhoods adjacency.')
parser.add_argument('--save_to', type=str,
        default='s3://cipta-bigdata1004/shape_join.csv',
        help='where to save the join.')
parser.add_argument('--loglevel', type=str, default='WARN',
                    help='log verbosity.')
args = parser.parse_args()

taxi_zones = json.load(open(args.taxi_shapes_path))
z_hoods = json.load(open(args.zillow_shapes_path))
t_edges = json.load(open(args.taxi_edges_path))
z_edges = json.load(open(args.zillow_edges_path))

class Neighborhood(object):
    def __init__(self, feature):
        super(Neighborhood, self).__init__()
        self.properties = feature['properties']
        self.geometry = feature['geometry']
        self.shape = shape(feature['geometry'])

z_hoods_shapes = [Neighborhood(f)
                  for f in z_hoods['features']
                  if f['properties']['City'] == 'New York']
taxi_zones_shapes = [Neighborhood(f) for f in taxi_zones['features']]

# Fit them into graphs.
T = nx.Graph()
T.add_edges_from(t_edges)
T = nx.freeze(T)
Z = nx.Graph()
Z.add_edges_from(z_edges)
Z = nx.freeze(Z)

regionid_to_z = {int(z.properties['RegionID']): z for z in z_hoods_shapes}
locid_to_t = {int(t.properties['LocationID']): t for t in taxi_zones_shapes}

def generate_connected_subgraphs(G):
    all_nodes = list(G)
    for sz in xrange(1, len(G)):
        for nodes in itertools.combinations(all_nodes, sz):
            subG = G.subgraph(nodes)
            if nx.is_connected(subG):
                yield subG
    yield G

def to_csv(l):
    '''Turns a tuple into a CSV row.
    Args:
        l: list/tuple to turn into a CSV
    Returns:
        (str) input encoded as CSV'''
    f = StringIO()
    writer = csv.writer(f)
    writer.writerow(l)
    return f.getvalue().strip()

def score_fn(shape_t, shape_z):
    inters = shape_t.intersection(shape_z).area
    union = shape_t.union(shape_z).area
    return inters / union

def analyze(z0_idx):
    '''Analyze the neighbors around a Zillow point.'''
    z0 = z_hoods_shapes[z0_idx]
    clusters = []

    z0_id = z0.properties['RegionID']
    test_pt = z0.shape.centroid
    near_zs = sorted(z_hoods_shapes[:],
            key=lambda z:test_pt.distance(z.shape.centroid))[:args.K]
    near_ts = sorted(taxi_zones_shapes[:],
            key=lambda t:test_pt.distance(t.shape.centroid))[:args.K]
    
    Znear = Z.subgraph([int(z.properties['RegionID']) for z in near_zs])
    Tnear = T.subgraph([int(t.properties['LocationID']) for t in near_ts])
    
    z_subgs = [list(x) for x in list(generate_connected_subgraphs(Znear))]
    t_subgs = [list(x) for x in list(generate_connected_subgraphs(Tnear))]

    # Special pickled import.
    from shapely.ops import cascaded_union
    for t_ids in t_subgs:
        t_shape = cascaded_union([locid_to_t[locid].shape for locid in t_ids])
        for z_ids in z_subgs:
            key = (tuple(sorted(t_ids)), tuple(sorted(z_ids)))
            z_shape = cascaded_union([regionid_to_z[regid].shape for regid in z_ids])
            score = score_fn(t_shape, z_shape)
            if score > threshold:
                clusters.append((key, score))

    return clusters

def format_cluster(cluster):
    ((t_ids, z_ids), score) = cluster
    t_ids = [str(x) for x in t_ids]
    z_ids = [str(x) for x in z_ids]
    return ('|'.join(t_ids), '|'.join(z_ids), score)

def main():
    conf = SparkConf().setAppName('shape_join')
    sc = SparkContext()
    sc.setLogLevel(args.loglevel)

    # Map of ([z_regions], [t_regions]) -> score.
    # clusters = {}
    threshold = 0.5
    print('-'*80 + '\n' + 'graph join' + '\n' + '-'*80)

    print('len(Z):', len(z_hoods_shapes))
    print('len(T):', len(taxi_zones_shapes))

    zs = sc.parallelize(range(len(z_hoods_shapes)), numSlices=len(z_hoods_shapes))
    clusters = zs.flatMap(analyze).reduceByKey(max)
    # cluster: [ ( (t_ids, z_ids), score ) ]

    print('Save to:', args.save_to)
    clusters.sortByKey()\
        .map(format_cluster)\
        .sortBy(lambda x: x[-1], ascending=False)\
        .map(to_csv)\
        .saveAsTextFile(args.save_to)

if __name__ == '__main__':
    main()

