from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson import json_util

from flask import Flask, request
from flask import jsonify
from flask.ext import restful
from flask.ext.restful import reqparse

from prod_settings import *

import json

# configure application
app = Flask(__name__)

# create database connection
try:
   client = MongoClient(HOST, PORT)
   client.ks.authenticate(USER_NAME, PASSWORD, mechanism=CREDENTIAL_MECHANISM)
   print "Connected successfully"
except ConnectionFailure, e:
   print "Cound not connect to MongoDB: %s" % e

# get collection
insp = client.ks.insp_objid

# initialize our api
api = restful.Api(app)


# Find most recent entries by location given by point and distance:
def queryRecentByLoc(lng, lat, dist=800):
        
    geoNear = { 
        "$geoNear": {
	    "near": { "type": "Point", "coordinates": [lng, lat] },
	    "distanceField": "dist.calculated",
            "maxDistance": dist,
	    "includeLocs": "place.location",
            "num": 5000,
	    "spherical": True
        }
    }

    group = { 
        "$group": {
	    "_id": "$place.place_id", 
            "last_inspection": { 
                "$max": { 
                    "_id": "$_id", "doctype": "$doctype", "inspection": "$inspection", "place": "$place" 
                } 
	     }, 
	     "dist": { "$max": "$dist" }
         }
    }

    sort = { "$sort": { "dist": 1 } }
    
    # execute aggregate pipeline
    pipeline = [geoNear, group, sort]
    rs = insp.aggregate(pipeline)

    inspections = []
    for result in rs:
        inspections.append(json.loads(json_util.dumps(result["last_inspection"])))

    return inspections
	

class HelloWorldAPI(restful.Resource):

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('name',
            type=str, required=True, location='args',
            help = "Argument 'name' is required")
        super(HelloWorldAPI, self).__init__()

    def get(self):
        args = self.reqparse.parse_args()
        return { 'hello': args['name'] }


class InspectionsByPlaceId(restful.Resource):

    def get(self, id):
	print "find inspections by place_id: {0}".format(id)
	rs = insp.find( { "place.place_id": id } )
        inspections = []
        for result in rs:
            print "found inspection for place_id: {0}".format(id)
            inspections.append(json.loads(json_util.dumps(result)))
        return inspections


class InspectionsByLoc(restful.Resource):

    def __init__(self):
        self.reqparse = reqparse.RequestParser()

	# point argument required
        self.reqparse.add_argument('pt',
            type=str, required=True, location='args',
            help = "Argument 'pt' is required: pt=<lng,lat>")

	# max distance from point
        self.reqparse.add_argument('dist',
            type=float, required=False, location='args', default=800.0,
            help = "Specify distance 'dist' in meters from 'pt' (optional): dist=<n>")

        super(InspectionsByLoc, self).__init__()

    def get(self):
        args = self.reqparse.parse_args()
	print "args: {0}".format(args)
        pt = args['pt'].split(",")
        dist = args['dist']
        lng = float(pt[0])
        lat = float(pt[1])
	results = queryRecentByLoc(lng, lat, dist)
        print "found {0} matched for location".format(len(results))
        return results


# Create routes
api.add_resource(HelloWorldAPI, '/hello')
api.add_resource(InspectionsByPlaceId, '/inspections/by_placeid/<string:id>')
api.add_resource(InspectionsByLoc, '/inspections/by_loc')


# default port is 5000
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=9090)
    #app.run(debug=False, host='0.0.0.0', port=9090)
    #app.run(debug=True)




