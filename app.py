import json

from usa_cities_db.handler import pipeline

def load_cities():
    pipeline.functions['load_cities'].invoke('')

def query_cell(h3_address):
    pipeline.functions['query_cell'].invoke({'h3_address': h3_address})


def query_polygon(geometry, resolution, compact=False):
    resp = pipeline.functions['query_polygon'].invoke(json.dumps({'geometry': geometry,
                                                'resolution': resolution,
                                                'compact': compact
                                                }))
    return resp