import requests
import json


class Client:

    def send(self, url, method='GET', data=None, headers={}):

        headers = headers.copy()
        headers['Content-Type'] = 'application/json'
        headers['Accept'] = 'application/json'
        if data:
            data = json.dumps(data)

        response = None
        status_code = None
        try:
            response = requests.request(url=url, method=method, data=data, headers=headers)
        except Exception as e:
            print("ERROR: Exception while requesting to the server: {}".format(e))
        else:
            status_code = response.status_code
            body = None
            try:
                body = response.data.decode('utf-8') #bytestream to dict
            except Exception as e:
                print ("DEBUG: Decode error on the response data: {}".format(e))
                pass

            if body is None:
                body = response.text

            return status_code, body

    def get(self, url, headers={}):
        return self.send(url, 'GET', headers=headers)

    def post(self, url, data, headers={}):
        return self.send(url, 'POST', data, headers=headers)

    def put(self, url, data, headers={}):
        return self.send(url, 'PUT', data, headers=headers)
