from flask import Flask
from flask import request
from flask import jsonify
from gevent.pywsgi import WSGIServer
from uszipcode import USZipCodes
from solcast import solcastData
from kafkadriver import kafkaConnector

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
    if 'dates' not in req:
        return {'status' : 'FAIL', 'result' : 'Missing required dates arguments'}
    
    coordinates = eapp.searchCoordinates(req)
    if len(coordinates) == 0:
        return {'status' : 'FAIL', 'result' : f"Coordinates are not found from {req}."}
     
    sdata = eapp.solcast.getSolarData(coordinates[0], coordinates[1], req['dates'])
    if sdata:
        return {'status' : 'SUCCESS', 'result' : sdata}
    else:
        return {'status' : 'FAIL', 'result' : f"Irradiance data not available for {coordinates} on {req['dates']}"}

@app.route('/predict', methods=['POST'])
def predict():
    req = request.get_json()
    # need to do the following
    # Based on load input signal, zip code (producer)
    # 1) Convert zip to coordinates
    # 2) Extract radiance data based on coordinates
    # 3) Post load input and radiance coordinates to kafka queue
    # 4) Classification model consumes the inputs and see if household has PV. If has PV, post the data to 'disaggregrate' 
    #    kafka queue (both producer and consumer)
    # 5) Disaggregation model consumes the inputs and predict the PV signals (consumer), once complete,
    #    a) post to anther update topic for other workers to update to PostgreSQL database
    #    b) update directly to PostgreSQL database
#data = {'load' : [0.0, 0.3, 0.4, 0.5, 0.1, 0.0], 'radiance' : [0.0, 0.0, 123.3, 222.1, 22.3, 0]}
#self.kafkaConn.send_message('classification', data)

 
if __name__ == "__main__":
    eapp = energyApps()
    http_server = WSGIServer(("0.0.0.0", 20100), app)
    http_server.serve_forever()
