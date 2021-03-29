#!/usr/bin/env python
# Kyle Fitzsimmons, 2020
from datetime import datetime
import json
import os
import psycopg2
import psycopg2.extras

import utils


DB = {
    'host': 'host address',
    'port': port_number (int),
    'user': 'postgres username',
    'password': 'postgres password',
    'dbname': 'database name'
}
OUTPUT_DATA_DIR = './output'

conn = psycopg2.connect(**DB)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

#The following function gets a table from postgresql db, and save it as a geojson file
def write_geojson_points(points):
    keys = [
        'id', 'survey_id', 'mobile_id', 'altitude', 'speed', 'direction',
        'h_accuracy', 'v_accuracy', 'acceleration_x', 'acceleration_y',
        'mode_detected', 'timestamp', 'point_type'
    ]
    features = []
    for p in points:
        features.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point', 
                'coordinates': [p['longitude'], p['latitude']]
            },
            'properties': {key: p[key] for key in keys}
        })
    collection = {'type': 'FeatureCollection', 'features': features}
    mobile_id = features[0]['properties']['mobile_id']
    geojson_fp = os.path.join(OUTPUT_DATA_DIR, f'{mobile_id}-coordinates.geojson')
    with open(geojson_fp, 'w') as geojson_f:
        geojson_f.write(json.dumps(collection, default=utils.json_serialize))

def main(mobile_id, start, end):
    if not os.path.exists(OUTPUT_DATA_DIR):
        os.mkdir(OUTPUT_DATA_DIR)

    query = '''SELECT * FROM table WHERE condition = %s AND timestamp BETWEEN %s AND %s;'''
    cur.execute(query, (mobile_id, start, end))
    coordinates = cur.fetchall()
    write_geojson_points(coordinates)

if __name__ == '__main__':
    mobile_id = 5869
    start_dt = datetime(2017, 9, 21, 0, 0, 0)
    end_dt = datetime(2017, 9, 21, 23, 59, 59)
    main(mobile_id, start_dt, end_dt)
