from couchbase.bucket import Bucket
from couchbase.views.iterator import View, Query

from flask import Flask, request
from flask import jsonify
from flask.ext import restful
from flask.ext.restful import reqparse

from prod_settings import *


# configure application
app = Flask(__name__)

# create couchbase connection
cb = Bucket(CB_URL+BUCKET)

# initialize our api
api = restful.Api(app)


# Find most recent entry for placeId:
def queryRecentByPlaceId(value):
    q = Query(group=True, reduce=True, key=value)
    return cb.query(DESIGN_DOC_INSPECTIONS, VIEW_BY_PLACEID, query=q)


# Find most recent entries by location given by bbox:
def queryRecentByLoc(lat1, lng1, lat2, lng2):
    q = Query(group=True, reduce=True, inclusive_end=True, limit=5000,
        mapkey_range=[[lat1,lng1], [lat2,lng2]] )
        #connection_timeout=60000
    return cb.query(DESIGN_DOC_INSPECTIONS, VIEW_BY_LOC, query=q)


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
        rs = queryRecentByPlaceId(id)
        inspections = []
        for result in rs:
            print "place_id: {0} value: {1}".format(id, result.value)
            inspections.append(result.value)
        return inspections


class InspectionsByLoc(restful.Resource):

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('bbox',
            type=str, required=True, location='args',
            help = "Argument 'bbox' is required: bbox=<lat1,lng1,lat2,lng2>")
        super(InspectionsByLoc, self).__init__()

    def get(self):
        args = self.reqparse.parse_args()
        bbox = args['bbox'].split(",")
        minLat = float(bbox[1])
        maxLat = float(bbox[3])
        rs = queryRecentByLoc(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
        inspections = []
        for result in rs:
            print "result.value: {0}".format(result.value)
            # filter results by latitude
            if  minLat < result.place.location.lat < maxLat :
                inspections.append(result.value)
        return inspections


# Create routes
api.add_resource(HelloWorldAPI, '/hello')
api.add_resource(InspectionsByPlaceId, '/inspections/by_placeid/<string:id>')
api.add_resource(InspectionsByLoc, '/inspections/by_loc')


# default port is 5000
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=9090)
    #app.run(debug=True)




