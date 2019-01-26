import json
from multiprocessing import Process, Pipe
from h3 import h3
import boto3

from pipeline import Pipeline, events, resources, functions

dynamodb_resource = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

min_res = 2
max_res = 8

class WorldCitiesTable(resources.DynamoDB):

    def __init__(self):
        super().__init__()
        self['Properties']['AttributeDefinitions'] = [
            {"AttributeName": "ParentCell",
             "AttributeType": "S"},
            {"AttributeName": "CellLocationIndex",
             "AttributeType": "S"},
        ]
        self['Properties']['KeySchema'] = [
            {"AttributeName": "ParentCell",
             "KeyType": "HASH"},
            {"AttributeName": "CellLocationIndex",
             "KeyType": "RANGE"}
        ]
        self['Properties']['ProvisionedThroughput']['WriteCapacityUnits'] = 200

db_table = WorldCitiesTable()

class WorldCitiesDatabase(Pipeline):

    @staticmethod
    def query_db_table(key_condition_expression, expression_attribute_values):
        resp = dynamodb_client.query(TableName="WorldCitiesTable",
                            KeyConditionExpression=key_condition_expression,
                            ExpressionAttributeValues=expression_attribute_values)
        return resp

    def __init__(self):
        super().__init__(resources=[db_table])

    @events.invoke
    @functions.timeout(900)
    @functions.memory(3008)
    def load_cities(self, event, context):
        with open('usa_cities.geojson', 'r') as geoj:
            cities = json.load(geoj)['features']
            _db_table = dynamodb_resource.Table(db_table.name)
            print("Loading database table")
            with _db_table.batch_writer() as db_batch:
                for idx, city in enumerate(cities):
                    city_id = str(idx)
                    if city['properties']['NAME']:
                        hexagons = h3.polyfill(city['geometry'], max_res, geo_json_conformant=True)
                        if len(hexagons) > 0:
                            for hex in hexagons:
                                parents = [h3.h3_to_parent(hex, x) for x in range(min_res, max_res+1)]
                                range_key = "#".join(parents)+"#{}".format(city_id)
                                db_item = {'ParentCell': "{}".format(h3.h3_get_base_cell(hex)),
                                           'CellLocationIndex': range_key,
                                           'CityName': city['properties']['NAME'],
                                           'CityID': city_id
                                           }
                                db_batch.put_item(Item=db_item)
                    if idx % 1000 == 0:
                        print("Processed {} cities".format(idx))

    @functions.memory(3008)
    @events.invoke
    def query_cell(self, event, context):
        cities = []
        resolution = h3.h3_get_resolution(event['h3_address'])
        base_cell = str(h3.h3_get_base_cell(event['h3_address']))

        if resolution < max_res:
            max_query_res = resolution
        else:
            max_query_res = max_res

        range_query = "#".join([h3.h3_to_parent(event['h3_address'], x) for x in range(min_res, max_query_res + 1)])
        key_condition_expression = "ParentCell = :parentcell AND begins_with(CellLocationIndex, :index)"
        expression_values = {
            ":parentcell": {"S": base_cell},
            ":index": {"S": range_query}
        }
        resp = self.query_db_table(key_condition_expression, expression_values)
        for item in resp['Items']:
            city = item['CityName']['S']
            if city not in cities:
                cities.append(city)
        return cities

    @functions.memory(3008)
    @events.http(path='worldcities/polygon', method='post', cors='true')
    def query_polygon(self, event, context):
        hexagons = h3.polyfill(event['geometry'], event['resolution'], geo_json_conformant=True)
        if event['compact']:
            hexagons = h3.compact(hexagons)

        processes = []
        parent_connections = []

        for item in hexagons:
            parent_conn, child_conn = Pipe()
            parent_connections.append(parent_conn)

            process = Process(target=self.query_cell_wrapper, args=(item, child_conn))
            processes.append(process)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        cities = []
        for parent_connection in parent_connections:
            for city in parent_connection.recv()[0]:
                if city not in cities:
                    cities.append(city)

        resp = {'statusCode': 200,
                'body': json.dumps({'cities': cities})}
        return resp


    def query_cell_wrapper(self, h3_address, conn):
        cities = self.functions['query_cell'].invoke({'h3_address': h3_address})
        conn.send([cities])
        conn.close()

pipeline = WorldCitiesDatabase()

"""Lambda handlers"""

load_cities = pipeline.load_cities
query_cell = pipeline.query_cell
query_polygon = pipeline.query_polygon

"""Deploy pipeline"""

def deploy():
    pipeline.role.add_action('lambda:InvokeFunction')
    pipeline.role.add_action('lambda:InvokeAsync')
    pipeline.role.add_resource('arn:aws:lambda:*')
    pipeline.deploy()
