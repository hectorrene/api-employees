from flask import Flask, request    
from flask_restful import Resource, Api
import json, boto3, threading, time 

app = Flask(__name__)
api = Api(app)

sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-2.amazonaws.com/723851871329/my-api-queue'

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

api.add_resource(Employees, '/employees')
api.add_resource(addEmployee, '/employees/add')
api.add_resource(updateEmployee, '/employees/update/<int:id>')
api.add_resource(deleteEmployee, '/employees/delete/<int:id>')
api.add_resource(CompanyEmployees, '/company/<int:company_id>/employees')

if __name__ == '__main__':
    threading.Thread(target=listen_to_sqs, daemon=True).start()
    app.run(host='0.0.0.0', port=5001)
