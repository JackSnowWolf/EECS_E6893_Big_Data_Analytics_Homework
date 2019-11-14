import pandas_gbq
from django.shortcuts import render
from google.oauth2 import service_account

# Make sure you have installed pandas-gbq at first;
# You can use the other way to query BigQuery.
# please have a look at
# https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-nodejs
# To get your credential

credentials = service_account.Credentials.from_service_account_file(
    '/home/huchong/Downloads/hardy-symbol-252200-25dbece318bf.json')


def hello(request):
    context = dict()
    context['content1'] = 'Hello World!'
    return render(request, 'helloworld.html', context)


def dashboard(request):
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "hardy-symbol-252200"

    SQL = "SELECT time, ai, data, good, movie, spark " \
          "FROM `hardy-symbol-252200.twitter_analysis.rstcnt` " \
          "LIMIT 8"
    df = pandas_gbq.read_gbq(SQL)
    df_list = df.to_dict('records')

    data_list = []
    for df_row in df_list:
        data_row = dict()
        data_row["Time"] = df_row["time"].strftime(format="%H:%M")
        df_row = dict(df_row)
        df_row.pop("time")
        data_row["count"] = df_row
        data_list.append(data_row)

    data = dict()
    data["data"] = data_list

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
