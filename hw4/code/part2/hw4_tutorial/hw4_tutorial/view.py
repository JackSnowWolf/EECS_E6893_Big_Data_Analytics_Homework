import pandas_gbq
from django.shortcuts import render
from google.oauth2 import service_account

# Make sure you have installed pandas-gbq at first;
# You can use the other way to query BigQuery.
# please have a look at
# https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-nodejs
# To get your credential

credentials = service_account.Credentials.from_service_account_file(
    '/home/huchong/HuChong/Columbia/'
    'EECS_E6893_Big_Data_Analytics/Homework/'
    'hw4/code/hardy-symbol-252200-bedb45223747.json')


def hello(request):
    context = dict()
    context['content1'] = 'Hello World!'
    return render(request, 'helloworld.html', context)


def dashboard(request):
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "Your-Project"

    SQL = ""
    df = pandas_gbq.read_gbq(SQL)

    data = {}

    '''
        TODO: Finish the SQL to query the data, it should be limited to 8 rows. 
        Then process them to format below:
        Format of data:
        {'data': [{'Time': hour:min, 'count': {'ai': xxx, 'data': xxx, 'good': xxx, 'movie': xxx, 'spark': xxx}},
                  {'Time': hour:min, 'count': {'ai': xxx, 'data': xxx, 'good': xxx, 'movie': xxx, 'spark': xxx}},
                  ...
                  ]
        }
    '''

    return render(request, 'dashboard.html', data)


def connection(request):
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "Your-Project"
    SQL1 = ''
    df1 = pandas_gbq.read_gbq(SQL1)

    SQL2 = ''
    df2 = pandas_gbq.read_gbq(SQL2)

    data = {}

    '''
        TODO: Finish the SQL to query the data, it should be limited to 8 rows. 
        Then process them to format below:
        Format of data:
        {'n': [xxx, xxx, xxx, xxx],
         'e': [{'source': xxx, 'target': xxx},
                {'source': xxx, 'target': xxx},
                ...
                ]
        }
    '''
    return render(request, 'connection.html', data)
