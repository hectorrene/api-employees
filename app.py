from flask import Flask, request    
from flask_restful import Resource, Api
import json, boto3, threading, time 

app = Flask(__name__)
api = Api(app)

sqs = boto3.client(
    'sqs',
    region_name='us-east-1',
    aws_access_key_id='ASIAUA4NCJJ5EXLOZPZU',
    aws_secret_access_key='MbEYaULuSgpG9ehBmj0OwA3KuZlRYw4h+L2N/YBS',
    aws_session_token='IQoJb3JpZ2luX2VjENn//////////wEaCXVzLXdlc3QtMiJHMEUCIHcCPNZ0cPt+5hC6fnwEcvfjUhc+lzzvdNUJAfD8KeOSAiEA8dZrO+NZFnTl9Fl1pZaAzGSsdpwOUcwzwS9f/Z9PYscqvgIIkv//////////ARAAGgwyNzY3ODQzNjgyNTAiDP8zzPNKnjY7WxM4TyqSAl39CDuR+BI11gV9iSBDt3B1NHv6UZc0AY4UHh+iMB/0lHIZyKSVJncifTZIhM3xuZqMhHc5ImDn4EAo5iN7uzi8XG4mUTzxO72Uar+7DezNZu31BafsdM5yq6JpMXeUBq1Bdf5ri8/D7l9JNVPikcQfe3emWmOANSEpQIXixgCC5E6DJj9iNnDLc9Pn+iEeNmV7omWRRGGkCHZ+BYI3lP/FBw7APjWVY2J2iMMP37eO+85QUP49NXZo1ZYXQb3CFiTXX2UujvFT0fH+0AY1WlPjMibH3LKP2ZKrZfndJGVn59YZdcHAUJrrOBCZjz1PFw4fuaK8pJ7CKIYd4WnrPt2W4YY7NYRtHODBcAtayqAOKiYwh7+twQY6nQGjg3eyO1ORCuOrIST5C2LL9eM9yNxZJHkFN6FcRHAFq8ajwQFd2vqGPQXq2tANPZcDuSSDyjd3+yj2WwKnqhoE326OVEkZvZEfip6XJTtfU8v5XuLRI9BanrT9T3gDadYd54xKqEr3DgzRCRU4pW8NdFabdbZTejVL3FElBXVhOeMVDmbntQFQkB/PaS7ukfjTByVJi1HCvguHWXky',
)

queue_url = 'https://sqs.us-east-1.amazonaws.com/276784368250/my-api-queue'

employees = { 
    1: {'name' : 'Oscar Mendivil', 'companyId': '1'},
    2: {'name' : 'Paola Díaz', 'companyId': '2'},
    3: {'name' : 'Héctor Hurtado', 'companyId': '3'},
    4: {'name' : 'André Acero', 'companyId': '4'},
    5: {'name' : 'Pedro Mendez', 'companyId': '5'},
}

def listen_to_sqs():
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1
            )

            messages = response.get('Messages', [])

            if not messages:
                print("[SQS] No new messages... waiting")
            
            for msg in messages:
                body = json.loads(msg['Body'])

                if body.get('action') == 'create_employee':
                    payload = body.get('payload', {})
                    name = payload.get('name')
                    company_id = payload.get('companyId')

                    if name and company_id:
                        new_id = max(employees.keys(), default=0) + 1
                        employees[new_id] = {
                            'name': name,
                            'companyId': company_id
                        }
                        print(f"[SQS] Added employee {name} to company {company_id} (ID: {new_id})")

                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=msg['ReceiptHandle']
                )

        except Exception as e:
            print(f"[SQS] Error receiving message: {e}")

        time.sleep(1)

class Employees(Resource):
    def get(self):
        return employees

class addEmployee(Resource):
    def post(self):
        data = request.get_json()
        new_id = max(employees.keys(), default=0) + 1
        employees[new_id] = {
            'name' : data.get('name'), 
        }

        return {'id': new_id, 'employee' : employees[new_id]}, 201

class updateEmployee (Resource):
    def put(self, id):
        if id in employees:
            data = request.get_json()
            employees[id]['name'] = data.get('name', employees[id]['name'])
            return {'message': 'Employee info updated', 'employee': employees[id]}, 200
        return {'message': 'Employee not found'}, 404

class deleteEmployee(Resource): 
    def delete(self, id):
        if id in employees:
            deleted = employees.pop(id)
            return {'message': 'Company deleted', 'deleted': deleted}, 200
        return {'message': 'Company not found'}, 404
    
class CompanyEmployees(Resource):
    def get(self, company_id):
        result = {emp_id: data for emp_id, data in employees.items() if data.get('companyId') == str(company_id)}
        
        if not result:
            return {'message': f'No employees found for company ID {company_id}'}, 404
        
        return result, 200

api.add_resource(Employees, '/')
api.add_resource(addEmployee, '/add')
api.add_resource(updateEmployee, '/update/<int:id>')
api.add_resource(deleteEmployee, '/delete/<int:id>')
api.add_resource(CompanyEmployees, '/company/<int:company_id>/employees')

if __name__ == '__main__':
    threading.Thread(target=listen_to_sqs, daemon=True).start()
    app.run(host='0.0.0.0', port=5001)
