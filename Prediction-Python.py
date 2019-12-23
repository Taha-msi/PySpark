# Polynomial Regression

# Importing the libraries
import numpy as np
import pandas as pd
from datetime import datetime as dt
from pymongo import MongoClient,InsertOne
from pymongo.errors import InvalidOperation
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVR
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor




connection={
        'host':'10.1.0.162',
        'port':27017,
        'user':'godson',
        'passw':'godson123',
        'source':'ktpisl'
        }

def get_db(host,port,passw,source,user):
    client = MongoClient(host,port,maxPoolSize =200,wTimeoutMS=2500)
    client.admin.authenticate(user, passw,source)
    db = client['ktptest']
    return db

def getSensorName():
    sensors = pd.read_csv('C:/Users/tahamansouri/Documents/My Reports/Prediction/sensor-names-etc.csv')
    sensors=sensors.to_dict('records')
    sensorList={}
    for sens in sensors:
        sensorList[sens['mapID']]=sens['name']
    return sensorList
        


mydb=get_db(connection['host'],connection['port'],connection['passw'],connection['source'],connection['user'])


# Importing the dataset

def importer(file):
    sensorList = getSensorName()
    dataset = pd.read_json('C:/Users/tahamansouri/Documents/My Reports/Prediction/'+file, lines=True)
    dataset=dataset.to_dict('records')
    bulk_inserts=[]
    for data in dataset:
        data['recordDate']=data['record']['recordDate']
        data['value']=data['record']['value']
        del data['record']
        date=dt.strptime(data['recordDate'].split('+')[0], "%Y-%m-%dT%H:%M:%S")
        data['recordDate']=date
        data['hour']=date.hour
        data['weekDay']=date.weekday()
        data['month']=date.month
        data['day']=date.day
        data['name']=sensorList[data['mapID']]
        insert=InsertOne(data)
        bulk_inserts.append(insert)
    try:
        mydb.vahidTest.bulk_write(bulk_inserts)
    except InvalidOperation:
        print("no updates necessary")
    except Exception as e:
        print("some thing is wrong")
        print(str(e))

def importDataAll():
    file = 'sensor-data-'
    for i in range(1,32):
        file2=file+str(i)+'.json'
        importer(file2)
    

def loadData(mapID):
    cursor= mydb.vahidTest.find({'mapID':mapID},{'_id': 0,'recordDate': 1,'hour': 1,'weekDay': 1,'month': 1,'day': 1,'value':1}).sort('recordDate',1)
    lis=[]
    for i in cursor:
        lis.append([i['recordDate'],i['hour'], i['weekDay'],i['month'],i['day'],i['value']])
    return np.array(lis)


def polynominalPredict():
    X = test[0:-nextStepRead, 1:6].tolist()
    y = test[nextStepRead:seriSize+1, 5].tolist()
    X_train=X[0:seriSize-predictPeriod-nextStepRead]
    X_test=X[seriSize-predictPeriod-nextStepRead:len(X)+1]
    y_train=y[0:seriSize-predictPeriod-nextStepRead]
    y_test=y[seriSize-predictPeriod-nextStepRead:len(y)+1]
    poly_reg = PolynomialFeatures(degree = degree)
    X_poly = poly_reg.fit_transform(X_train)
    poly_reg.fit(X_poly,y_train)
    lin_reg_2 = LinearRegression()
    lin_reg_2.fit(X_poly, y_train)
    y_predict=lin_reg_2.predict(poly_reg.fit_transform(X_test))
    return y_test, y_predict.reshape(-1,1)

def svrPredict():
    sc_X = StandardScaler()
    sc_y = StandardScaler()
    X = test[0:-nextStepRead, 1:6].tolist()
    y = test[nextStepRead:seriSize+1, 5].tolist()
    X_s = sc_X.fit_transform(X)
    y_s = sc_y.fit_transform(np.array(y).reshape(-1,1))
    regressor = SVR(kernel = 'rbf',gamma='auto')
    X_train=X_s[0:seriSize-predictPeriod-nextStepRead]
    X_test=X_s[seriSize-predictPeriod-nextStepRead:len(X)+1]
    y_train=y_s[0:seriSize-predictPeriod-nextStepRead]
    y_test=y[seriSize-predictPeriod-nextStepRead:len(y)+1]
    # Modeling
    regressor.fit(X_train, np.ravel(y_train))
    # Predicting a new result
    y_pred = regressor.predict(X_test)
    y_pred = sc_y.inverse_transform(y_pred)
    return y_test, y_pred


def randomForestPredict():
    X = test[0:-nextStepRead, 1:6].tolist()
    y = test[nextStepRead:seriSize+1, 5].tolist()
    X_train=X[0:seriSize-predictPeriod-nextStepRead]
    X_test=X[seriSize-predictPeriod-nextStepRead:len(X)+1]
    y_train=y[0:seriSize-predictPeriod-nextStepRead]
    y_test=y[seriSize-predictPeriod-nextStepRead:len(y)+1]
    regressor = RandomForestRegressor(n_estimators = 300, random_state = 0)
    regressor.fit(X_train, y_train)
    y_pred = regressor.predict(X_test)
    return y_test, y_pred


mapID=134609
test= loadData(mapID)

nextStepRead=1
seriSize=len(test)
predictPeriod=30
degree=2

actual,predicted = polynominalPredict()
actual,predicted = svrPredict()

actual,predicted = randomForestPredict()

