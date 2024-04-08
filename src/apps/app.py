from flask import Flask
from flask import request
from flask import jsonify
from gevent.pywsgi import WSGIServer
from uszipcode import USZipCodes
from solcast import solcastData
from kafkadriver import kafkaConnector
import pdb
#import pgAPI as pgapi

class energyApps:
    def __init__(self):
        self.locale = USZipCodes()
        self.solcast = solcastData()
        self.kafkaConn = kafkaConnector()
        self.kafkaConn.start_producer()

    def searchCoordinates(self, req):
        if 'zip' in req:
            coordinates = self.locale.getCoordinatesByZip(req['zip'])
        elif 'county' in req: 
            coordinates = self.locale.getCoordinatesByCounty(req['county'])
        elif 'city' in req and 'state' in req:
            coordinates = self.locale.getCoordinatesByCity(req['city'], req['state'])
        elif 'state' in req:
            coordinates = self.locale.getCoordinatesByState(req['state'])
        else:
            return []
        return coordinates

app = Flask(__name__)

@app.route("/")
def ping() -> str:
    return "I am still alive."

@app.route('/lookupCoordinates', methods=['POST'])
def getCoordinates():
    req = request.get_json()
    result = eapp.searchCoordinates(req)
    if result:
        return {'status' : 'SUCCESS', 'result' : result}
    else:
        return {'status' : 'FAIL', 'result' : 'Missing required arguments'}

@app.route('/updateRadianceData', methods=['POST'])
def updateRadiance():
    req = request.get_json()
    if 'startdate' not in req:
        return {'status' : 'FAIL', 'result' : 'Missing required startdate YYYY-MM-DD arguments'}

    if 'duration' in req:
        duration = req['duration']
    else:
        # default is one day
        duration = 1

    coordinates = eapp.searchCoordinates(req)
    if len(coordinates) == 0:
        return {'status' : 'FAIL', 'result' : f"Coordinates are not found from {req}."}

    status = eapp.solcast.updateSolarData(coordinates[0], coordinates[1], req['startdate'], duration)
    if status:
        return {'status' : 'SUCCESS', 'result' : f"Coordinates {coordinates} updated in cache successfully for start date on {req['startdate']} with duration [{duration}]."}
    else:
        return {'status' : 'FAIL', 'result' : f"Unable to update Radiance data for coordinates {coordinates} for start date on {startdate} with duration [{duration}]."}

@app.route('/getRadianceData', methods=['POST'])
def getRadiance():
    req = request.get_json()
    coordinates = eapp.searchCoordinates(req)
    if len(coordinates) == 0:
        return {'status' : 'FAIL', 'result' : f"Coordinates are not found from {req}."}
     
    try:
        dates = req['dates']
    except:
        dates = []
    sdata = eapp.solcast.getSolarData(coordinates[0], coordinates[1], dates)
    if sdata:
        return {'status' : 'SUCCESS', 'result' : sdata}
    else:
        return {'status' : 'FAIL', 'result' : f"Irradiance data not available for {coordinates} on {dates}"}

@app.route('/predict', methods=['POST'])
def predict():
    req = request.get_json()
    try:
        eapp.kafkaConn.send_message('classification', req)
        print(f"Predict request posted. Net:{len(req['net'])}, Irr:{len(req['irradiance'])}")
        return {'status' : 'SUCCESS', 'result' : "Request posted."}
    except:
        return {'status' : 'FAIL', 'result' : "Request failed."}

@app.route('/getStatsByHouse', methods=['POST'])
def getStatsByHouse():
    try:
        req = request.get_json()
        house_id = req['house_id']
        timestamp = req['timestamp']
        #print(pgapi.getLastRecordingByHouse(house_id, timestamp))
        #last_pv, last_net_load = pgapi.getLastRecordingByHouse(house_id, timestamp)
        print(last_pv, last_net_load)
        #gross_load = pgapi.getGrossLoad(last_pv, last_net_load)
        print(gross_load)
        #pv_penetration = pgapi.getPVPenetration(last_pv, gross_load)
        print(pv_penetration)
        result = { 'gross_load' : gross_load, 'pv_penetration' : pv_penetration }

        return {'status' : 'SUCCESS', 'result' : result}
    except Exception as e:
        return {'status' : 'FAIL', 'result' : 'Request failed. ' + e}
"""
@app.route('/getStatsByCounty', methods=['POST'])
def getStatsByCounty():
    try:
        req = request.get_json()
        county_id = req['county_id']
        #timestamp = req['timestamp']
        #print(pgapi.getLastRecordingByCounty(county_id, timestamp))
        print(pgapi.getLastRecordingByCounty(county_id))
        return {'status' : 'SUCCESS', 'result' : 'Request posted.'}
    except:
        return {'status' : 'FAIL', 'result' : 'Request failed.'}

@app.route('/getStatsByState', methods=['POST'])
def getStatsByState():
    try:
        req = request.get_json()
        state_id = req['state_id']
        #timestamp = req['timestamp']
        #print(pgapi.getLastRecording(house_id, timestamp))
        return {'status' : 'SUCCESS', 'result' : 'Request posted.'}
    except:
        return {'status' : 'FAIL', 'result' : 'Request failed.'}
"""
if __name__ == "__main__":
    eapp = energyApps()
    http_server = WSGIServer(("0.0.0.0", 20100), app)
    print(f"Energy Data Service started @ port 20100")
    http_server.serve_forever()
