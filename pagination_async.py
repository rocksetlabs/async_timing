import os
import requests
import sys
from datetime import datetime
import json
import time
import random

class Processor():
    def __init__(self, workspace, queryLambda, tag=None, version=None, parameters = None, page_size=100):
        self.apiServer = os.getenv('ROCKSET_APISERVER')
        self.apiKey = os.getenv('ROCKSET_APIKEY')
        self.baseURL = self.apiServer + '/v1/orgs/self/ws'
        self.headers = {'Authorization': 'ApiKey ' + self.apiKey , 'Content-Type': 'application/json'}
        self.initialLimit = page_size
        self.pagination = True
        self.async_query = True
        self.workspace = workspace
        self.queryLambda = queryLambda
        self.tag = tag
        self.version = version
        self.finalUrl = ""
        self.parameters = parameters
        self.query_id = ""
        self.run()

    def run(self):
        self.status = ""
        self.start_time = datetime.utcnow()
        payload = {}
        payload['async_options'] = {}
        payload['async_options']['client_timeout_ms'] = 1
        payload['async_options']['timeout_ms'] = 1800000
        payload['async_options']['max_initial_results'] = self.initialLimit
        payload['parameters'] = []
        # payload['parameters'] = []
        payload['parameters'] = self.parameters
        self.finalUrl = self.buildURL()
        r = requests.post(self.finalUrl,
                            json=payload,
                            headers=self.headers)
        self.elapsed_time = datetime.utcnow() - self.start_time
        if r.status_code != 200:
          print(f'Failed to execute query. Code: {r.status_code}. {r.reason}. {r.text}')
          sys.exit(0)


        self.result = r.json()
        print("Query Id: {}".format(self.result['query_id']))
        self.query_id = self.result['query_id']
        self.status = self.result['status']
        while self.status != "DONE":
            if 'query_errors' in self.result.keys():
                print('Error: {}'.format(self.result['query_errors'] ))
            else:
                if self.status == 'RUNNING':
                    self.check_status(self.query_id)
                elif self.status == 'QUEUED':
                    self.check_status(self.query_id)
                elif self.status == 'COMPLETED':
                    self.elapsed_time = datetime.utcnow() - self.start_time
                    print("Query Time: {}".format(self.elapsed_time))
                    if self.pagination:
                        if 'data' not in self.result.keys():
                            self.totalResults = self.result['results_total_doc_count']
                            current_doc_count = self.result['pagination']['current_page_doc_count']
                            # print(f'Total doc count: {self.totalResults}, current_doc_count: {current_doc_count} time: {self.elapsed_time}')
                            self.iterate_query(self.query_id, self.result['pagination']['start_cursor'])
                        else:
                            # print(self.result['data'])
                            self.totalResults = self.result['data']['stats']['result_set_document_count']
                            # print(f'Total doc count: {self.totalResults}, time: {self.elapsed_time}')
                            self.iterate_query(self.query_id, self.result['data']['pagination']['start_cursor'])
                    else:
                        print(f'Time: {self.elapsed_time}')
                    self.status = 'DONE'
                else:
                    print("got some other status: {}".format(self.result['data']['status']))
                    sys.exit(1)
        print("Total Time: {}".format(datetime.utcnow() - self.start_time))

    def check_status(self, query_id):
        time.sleep(1)
        url = "{}/v1/orgs/self/queries/{}".format(self.apiServer, query_id)
        g = requests.get(url,
                        headers=self.headers)
        if g.status_code != 200:
          print(f'Failed to execute query. Code: {g.status_code}. {g.reason}. {g.text}')
          sys.exit(0)
        self.result = g.json()
        self.status = self.result['data']['status']

    def iterate_query(self, query_id, next_cursor):
        s_time = datetime.utcnow()
        url = "{}/v1/orgs/self/queries/{}/pages/{}?docs={}".format(self.apiServer, query_id, next_cursor, self.initialLimit)
        g = requests.get(url,
                        headers=self.headers)
        if g.status_code != 200:
          print(f'Failed to execute query. Code: {g.status_code}. {g.reason}. {g.text}')
          sys.exit(0)
        self.result = g.json()
        if 'next_cursor' not in self.result['pagination'] or self.result['pagination']['next_cursor'] == None:
            pass
            # print("Total Time: {}".format(datetime.utcnow() - self.start_time))
        else:
            current_doc_count = self.result['pagination']['current_page_doc_count']
            e_time = datetime.utcnow() - s_time
            # print(f'current_doc_count: {current_doc_count} time: {e_time}')
            self.iterate_query(query_id, self.result['pagination']['next_cursor'])


    def buildURL(self):
        if self.version != None:
            url = "{}/{}/lambdas/{}/versions/{}".format(self.baseURL,
                                                        self.workspace,
                                                        self.queryLambda,
                                                        self.version)
        elif self.tag != None:
            url = "{}/{}/lambdas/{}/tags/{}".format(self.baseURL,
                                                    self.workspace,
                                                    self.queryLambda,
                                                    self.tag)
        else:
            raise Exception
        return url

class Config():
    def __init__(self, path):
        self.path = path
        self.data = {}
        self.load()

    def load(self):
        with open(self.path, 'r') as f:
            data = f.read()
        self.data = json.loads(data)

    def random_param(self):
        len_params = len(self.data) - 1
        index = random.randint(0, len_params)
        return self.data[index]



if __name__ == '__main__':
    config = Config("./parameters.json")
    for x in range(0, 2):
        processor = Processor('taxi', 'limit-offset-test', 'latest', None, config.random_param()['parameters'], 10)
