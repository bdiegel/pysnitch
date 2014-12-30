
from flask import Flask, request
from flask import jsonify
from flask.ext import restful
from flask.ext.restful import reqparse

from couchbase import Couchbase
from couchbase.views.params import Query
from couchbase.exceptions import CouchbaseError


# configure application
app = Flask(__name__)
app.config.from_pyfile('prod_settings.py')

# create couchbase connection
cb = Couchbase.connect(bucket=app.config['DATABASE'], host=app.config['HOST'])

# initialize our api
api = restful.Api(app)

# try:
#     result = c.get("ff729551-8f91-4929-abaf-b80579ce76a1")
#
# except CouchbaseError as e:
#     print "Couldn't retrieve value for key", e
#     # Rethrow the exception, making the application exit
#     raise
#
# doc = result.value
#
# print unicode("Name: {name}, Score: {score}").format(name=doc['name'], score=doc['score'])


class HelloWorldAPI(restful.Resource):

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('name', type = str, required = True,
            help = 'No name argument provided', location = 'args')
        super(HelloWorldAPI, self).__init__()

    def get(self):
        args = self.reqparse.parse_args()
        # name = request.args.get("name")
        return { 'hello' : args['name']}


class InspectionsAPI(restful.Resource):

    def get(self, id):
        doc = cb.get(id)
        print "key: {0}\nvalue: {1}".format(doc.key, doc.value)
        return doc.value


class InspectionsByNameAPI(restful.Resource):

    def get(self, name):

        mapkey_range = [[name], [name, Query.STRING_RANGE_END]]
        view_results = cb.query(app.config['DESIGN_DOC_INSPECTIONS'], app.config['VIEW_INSPECTIONS_BY_NAME'], mapkey_range=mapkey_range)

        inspections = []
        for result in view_results:
            print result
            inspections.append({ 'id' : result.docid, 'date' : result.key[1:4], 'score': result.value })

        return { 'name': name, 'inspections':  inspections }


# Create routes
api.add_resource(HelloWorldAPI, '/hello')
api.add_resource(InspectionsAPI, '/inspections/<string:id>')
api.add_resource(InspectionsByNameAPI, '/inspections/by_name/<string:name>')


if __name__ == '__main__':
    app.run(debug=True)