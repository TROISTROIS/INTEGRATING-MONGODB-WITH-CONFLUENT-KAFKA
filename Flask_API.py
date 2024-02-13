from flask import Flask, jsonify,request, Response, render_template
from pymongo import MongoClient
from bson.json_util import dumps
from bson.objectid import ObjectId
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.errors import CollectionInvalid, OperationFailure

conn_string = ""

# Connect to MongoDB
client = MongoClient(conn_string)
# Select the database
db = client['']
collection = db['']

app = Flask(__name__)

#. Read Operation to get all available Gps Providers
@app.route('/gps_providers', methods=['GET'])
def get_gps_providers():
    try:
        documents = collection.find()
        # Initialize an empty set to collect all unique keys
        distinct_keys = set()
        # Iterate over each document and collect its keys
        for document in documents:
            distinct_keys.update(document.keys())
        # Check if 'GpsProvider' column exists in the collection
        if 'GpsProvider' not in distinct_keys:
            return jsonify({'error': 'Column "GpsProvider" not found'}), 404
        # Retrieve all unique GPS providers
        gps_providers = collection.distinct('GpsProvider')
        # Check if there are no GPS providers available
        if not gps_providers:
            return jsonify({'message': 'No GPS providers found'}), 204

        # Return GPS providers
        return jsonify(gps_providers), 200

    except CollectionInvalid:
        return jsonify({'error': 'Invalid collection name'}), 500

    except OperationFailure as e:
        return jsonify({'error': str(e)}), 500

#2. Read Operation to filter information
# Get all documents with a specific vehicle Number
@app.route('/api/vehicleno', methods=['GET'])
def vehicleno():
    try:
        # Get the value of the 'vehicle_no' query parameter
        vehicle_no = request.args.get('vehicle_no')

        if not vehicle_no:
            # Print an error message indicating that the parameter is missing in the URL
            print("Missing 'vehicle_no' parameter in the URL, now checking in the database")

        # Example: Get documents with a specific 'vehicle_no' from the database
        result = collection.find({'vehicle_no': "TN19AH6070"})

        # Convert the result to a list, and convert ObjectId to string for JSON serialization
        result_list = [{'_id': str(doc['_id']), 'vehicle_no': doc['vehicle_no']} for doc in result]

        if result_list:
            return jsonify({"result": result_list}), 200
        else:
            return jsonify({"result": "No matching documents found"}), 404

    except Exception as e:
        # Check if the exception has a meaningful error message
        error_message = str(e) if e and str(e) else "An unexpected error occurred."

        # Return a JSON response with the error message and a 500 Internal Server Error status code
        return jsonify({"error": error_message} if error_message else {"error": "An unexpected error occurred."}), 500

#3. read operation
# get a driver name for a specific vehicle number

@app.route('/api/drivers/<vehicle_number>', methods=['GET'])
def get_drivers_by_vehicle_number(vehicle_number):
    try:
        # Check if the collection is empty
        if collection.count_documents({}) == 0:
            return jsonify({'message': 'Collection is empty'}), 404

        # Check if 'vehicle_no' and 'Driver_Name' fields exist in the collection
        sample_document = collection.find_one()
        if sample_document is not None and 'vehicle_no' in sample_document and 'Driver_Name' in sample_document:
            # Retrieve drivers for the given vehicle number
            drivers = list(collection.find({'vehicle_no': vehicle_number}, {'Driver_Name': 1, '_id': 0}))

            # Check if the vehicle number is not found
            if not drivers:
                return jsonify({'message': f'No drivers found for vehicle number {vehicle_number}'}), 404

            # Extract unique driver names from the list of documents
            driver_names = set()
            for driver in drivers:
                driver_names.add(driver['Driver_Name'])

            return jsonify({'drivers': list(driver_names)}), 200
        else:
            return jsonify({'error': 'Fields "vehicle_no" or "Driver_Name" not found in the collection'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# POST method to add a document to the collection
@app.route('/api/new_document', methods=['POST'])
def add_new_document():
    try:
        new_json = request.json
        # Validate the request data
        if not new_json:
            return jsonify({'error': 'Request data is missing'}), 400

        # Insert the document into the collection
        result = collection.insert_one(new_json)

        if result.inserted_id:
            return jsonify({'message': 'Document added successfully', 'inserted_id': str(result.inserted_id)}), 201
        else:
            return jsonify({'error': 'Failed to add the document'}), 500

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/delete_document/<id>', methods=['DELETE'])
def delete_document(id):
    try:
        # Check if the document exists
        if not collection.find_one({'_id': ObjectId(id)}):
            return jsonify({'error': 'Document not found'}), 404

        # Delete the document from the collection
        result = collection.delete_one({'_id': ObjectId(id)})

        if result.deleted_count > 0:
            return jsonify({'message': 'Document deleted successfully'}), 200
        else:
            return jsonify({'error': 'Failed to delete the document'}), 500

    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/api/update_document/<id>', methods=['PUT'])
def update_name(id):
    try:
        # Check if the document exists
        if not collection.find_one({'_id': ObjectId(id)}):
            return jsonify({'error': 'Document not found'}), 404

        # Update the name of the driver to "Ezumalai"
        result = collection.update_one({'_id': ObjectId(id)}, {'$set': {'Driver_Name': 'Ezumalai'}})

        if result.modified_count > 0:
            return jsonify({'message': 'Driver name updated to "Ezumalai" successfully'}), 200
        else:
            return jsonify({'error': 'Failed to update the driver name'}), 500

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/count', methods=['GET'])
def count_vehicles_by_gps_provider():
    try:
        # Aggregate the count of vehicles by GpsProvider
        pipeline = [
            {"$group": {"_id": "$GpsProvider", "count": {"$sum": 1}}}
        ]
        result = list(collection.aggregate(pipeline))

        if result:
            return jsonify({"result": result}), 200
        else:
            return jsonify({"result": "No data available"}), 404

    except Exception as e:
        # Check if the exception has a meaningful error message
        error_message = str(e) if e and str(e) else "An unexpected error occurred."

        # Return a JSON response with the error message and a 500 Internal Server Error status code
        return jsonify({"error": error_message} if error_message else {"error": "An unexpected error occurred."}), 500


if __name__ == '__main__':
    app.run(debug=True)

