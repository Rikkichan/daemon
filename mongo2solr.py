from pymongo import *
from datetime import datetime, timedelta

import sys
import daemon
import requests
import time
import logging
import logging.config
import bson
import json
import argparse
import pymongo.errors as mongoErrors
from queue import Queue
from threading import Thread
from bson.json_util import dumps, loads


#определение очереди для изменения значений в Solr
#Приоритетная очередь для загрузок из базы и других запросов
__priorityQueue__ = Queue()
#Второстепенная очередь 
__secondaryQueue__ = Queue()
#Булевая переменная для определения завершения обновления базы
isSendOnlyPermissions=True


def getFileNoInLoggers(logging,logger):
	getLoggingFiles = logging.root.handlers
	getLoggerFiles = logger.handlers
	result=[]
	for i in (getLoggingFiles+getLoggerFiles):
		if(str(type(i))=="<class 'logging.FileHandler'>"):
			result.append( i.stream.fileno())
	return result


parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config")
argumentSys = parser.parse_args(sys.argv[1:])
fileConfig = open(argumentSys.config, "r")
myConfig = json.loads(fileConfig.read())
fileConfig.close()
#инициализация логов
logging.config.dictConfig(myConfig["logConfig"])
#определение корневого элемента
logger = logging.getLogger("App")
#логирование запуска скрипта
logger.info("start program")
#определение файлов, которые не должны закрываться после запуска демона
files = getFileNoInLoggers(logging, logger)
#булева переменная для остановки потока Observer'а
thread_break = False


#основной поток демона
def main():
	global isSendOnlyPermissions
	global thread_break
	logger = logging.getLogger("App.main")
    #тайм-аут для переподключение к Mongo
	connect_to_mongo = 2
    #определение потока отправки данных на Solr
	rec = Thread(target = queue_into_request)
    #запуск потока
	rec.start()
    #основной цикл программы для подключения
	while (True):
		try:
			#Даём переменной загрузки из бд приоритет
			isSendOnlyPermissions = True
            #булева переменная для остановки потока
			thread_break = False
			logger.info("Connecting to mongodb: " + myConfig["mongoClientIP"] )
            #определение подключения к Mongo
			client = MongoClient(myConfig["mongoClientIP"], replicaset = myConfig["mongoClientReplicaSet"])
			logger.info("Connected to mongodb: " + myConfig["mongoClientIP"])
            #определение URL Solr
			solr_url = myConfig["solrURL"]
            #определение коллекции Solr
			solr_collection = myConfig["solrCollection"]
            #определение базы в Mongo
			db = client[myConfig["mongoClientDB"]]
            #определение коллекции в Mongo
			collection = db[myConfig["mongoClientCollection"]]
			#запрос на получение всех элементов с mongodb и даты поcледнего изменения в solr (определение второго потока)
			observer = Thread(target = observer_collection, args = (collection, solr_url, solr_collection))
            #запуск второго потока
			observer.start()
            #определение способа обновления базы с помощью цикла
			if myConfig["force"]:
                #запуск жесткого обновления базы
				force_update_database(collection, solr_url, solr_collection)
			else:
                #запуск стандартного обновления базы
				update_database(collection, solr_url, solr_collection)
			#print("Завершил загрузку базы")
			#Все преоритетные запросы были переданы в очередь с приоритетом 
			isSendOnlyPermissions=False
            #ожидание завершения потока
			observer.join()
        #обработка исключений
		except requests.exceptions.HTTPError as e:
			logger.error(e)
		except  requests.exceptions.ConnectionError as e:
			logger.error(e)
		except mongoErrors.AutoReconnect as e:
			logger.error(e)
		except mongoErrors.BulkWriteError as e:
			logger.error(e)
		except mongoErrors.CollectionInvalid as e:
			logger.error(e)
		except mongoErrors.ConfigurationError as e:
			logger.error(e)
		except mongoErrors.ConnectionFailure as e:
			logger.error(e)
		except mongoErrors.CursorNotFound as e:
			logger.error(e)
		except mongoErrors.DocumentTooLarge as e:
			logger.error(e)
		except mongoErrors.DuplicateKeyError as e:
			logger.error(e)
		except mongoErrors.EncryptionError as e:
			logger.error(e)
		except mongoErrors.ExceededMaxWaiters as e:
			logger.error(e)
		except mongoErrors.ExecutionTimeout as e:
			logger.error(e)
		except mongoErrors.InvalidName as e:
			logger.error(e)
		except mongoErrors.InvalidOperation as e:
			logger.error(e)
		except mongoErrors.InvalidURI as e:
			logger.error(e)
		except mongoErrors.NetworkTimeout as e:
			logger.error(e)
		except mongoErrors.NotMasterError as e:
			logger.error(e)
		except mongoErrors.OperationFailure as e:
			logger.error(e)
		except mongoErrors.ProtocolError as e:
			logger.error(e)
		except mongoErrors.PyMongoError as e:
			logger.error(e)
		except mongoErrors.ServerSelectionTimeoutError as e:
			logger.error(e)
		except mongoErrors.WTimeoutError as e:
			logger.error(e)
		except mongoErrors.WriteConcernError as e:
			logger.error(e)
		except mongoErrors.WriteError as e:
			logger.error(e)
		except Exception as e:
			#print("error", e)
			logger.error(e)
		finally:
			thread_break = True
			while (observer.isAlive()):
				time.sleep(2)
			#Даём переменной загрузки из бд приоритет, чтобы можно было очистить очередь
			isSendOnlyPermissions = True
			#print("finally")
			logger.info('finally')
			#Очищаем очередь от недогруженных данных, потому что, потом проще загрузить из бд
			#Так как, запросы к бд нельзя делать до того, как загрузятся данные из очереди
			#Так как могут появиться ошибки вида конкурирующего потока или двойного добавления 
			while(not __secondaryQueue__.empty()):
				___secondaryQueue__.get()
			

#функция стандартного обновления базы
def update_database(collection, solr_url, solr_collection):
    #определение заголовка запроса Solr
	headers = {'Content-type': "application/json"}
    #строка запроса Solr
	URL = solr_url + '/' + solr_collection + '/update/json/docs?commit=true'
	firstResponse = get_documents_by_query(solr_url, solr_collection, "q=*:*&rows=0")
	if (firstResponse["response"]["numFound"] > 0): 
    	#отправка запроса к Solr на получение последнего обновленного документа
		response = get_documents_by_query(solr_url, solr_collection, "q=" + myConfig["fieldLastModificationDateFromSolr"] + ":*&rows=1&sort=" + myConfig["fieldLastModificationDateFromSolr"] + "+desc")
    	#получение IDMongo всех документов 
		responseForDelete = get_documents_by_query(solr_url, solr_collection, "q=" + myConfig["fieldCommonIDMongoInSolr"] + ":*&fl=" + myConfig["fieldCommonIDMongoInSolr"] + "&rows=" + str(response["response"]["numFound"]))
    	#получение всех IDMongo
		mongoForDelete = list(collection.find({}, {myConfig["fieldIDMongo"]:1}))
    	#формирование множества из IDSolr
		solrSetDelete = set([ i.get(myConfig["fieldCommonIDMongoInSolr"])[0] for i in responseForDelete['response']['docs']])
    	#формирование множества из IDMongo
		mongoSetDelete = set([ str(i.get(myConfig["fieldIDMongo"])) for i in mongoForDelete])
    	#цикл по всем элементам, которые находятся в Solr, но не находятся в Mongo
		for resDel in solrSetDelete.difference(mongoSetDelete):
        	#удаление элементов из Solr
			__priorityQueue__.put({"URL": solr_url + '/' + solr_collection + '/update?commit=true', "headers":headers, "data":dumps({"delete":{"query": myConfig["fieldCommonIDMongoInSolr"] + ":"+ resDel}})})
        	#получение даты последней обновленной записи (в Solr)
		date = response["response"]["docs"][0][myConfig["fieldLastModificationDateFromSolr"]][0]
        	#поиск записи в Mongo c датой обновления более поздней, чем дата обновления последней записи в Solr
		a= collection.find({myConfig["fieldLastModificationDateFromMongo"]: {"$gt": datetime.fromtimestamp(date / 1e3)-timedelta(hours = 5)} })
		#print(datetime.fromtimestamp(date / 1e3))
    	#цикл на добавление записей
		for coll in a:
			#print(coll)
            	#удаление возможной записи с IDMongo
			__priorityQueue__.put({"URL": solr_url + '/' + solr_collection + '/update?commit=true', "headers":headers, "data":dumps({"delete":{"query":"" + myConfig["fieldCommonIDMongoInSolr"] + ":"+str(coll[myConfig["fieldIDMongo"]])}})})
        	#добавление элемента
			__priorityQueue__.put({"URL": URL,"headers": headers, "data": dumps(coll)})
	else:
		val = collection.find()
		for v in val:
			__priorityQueue__.put({"URL": URL,"headers": headers, "data": dumps(v)})

#функция жесткого обновления базы
def force_update_database(collection, solr_url, solr_collection):
    #определение заголовка запроса Solr
	headers = {'Content-type': "application/json"}
    #строка запроса Solr
	URL = solr_url + '/' + solr_collection + '/update/json/docs?commit=true'
    #отправка запроса к Solr на получение количества всех документов
	response = get_documents_by_query(solr_url, solr_collection, "q=*:*&rows=0")
	if (response["response"]["numFound"] > 0):
    	#получение ID и даты каждой записи в Solr
		responseForDelete = get_documents_by_query(solr_url, solr_collection, "q=" + myConfig["fieldLastModificationDateFromSolr"] + ":*+%26%26+" + myConfig["fieldCommonIDMongoInSolr"] + ":*&fl=" + myConfig["fieldCommonIDMongoInSolr"] + "+" + myConfig["fieldLastModificationDateFromSolr"] + "&rows=" + str(response["response"]["numFound"]))
    	#получение ID и даты каждой записи в Mongo
		responseForMongo = collection.find({}, {myConfig["fieldIDMongo"]:1, myConfig["fieldLastModificationDateFromMongo"]:1})
    	#создание словаря для ID и даты в Solr
		dictSolr = {i.get(myConfig["fieldCommonIDMongoInSolr"])[0]:i.get(myConfig["fieldLastModificationDateFromSolr"])[0] for i in responseForDelete["response"]["docs"]}
    	#создание словаря для ID и даты в Mongo
		dictMongo = {str(i.get(myConfig["fieldIDMongo"])):i.get(myConfig["fieldLastModificationDateFromMongo"]) for i in list(responseForMongo)}
    	#создание множества из ID Solr
		solrSetIDs = set(dictSolr.keys())
    	#создание множества из ID Mongo
		mongoSetIDs = set(dictMongo.keys())
    	#определения множества на удаление в Solr (есть в Solr, но нет в Mongo)
		deleteSetForSolr = solrSetIDs.difference(mongoSetIDs)
    	#определения множества на добавление в Solr (есть в Mongo, но нет в Solr)
		addSetForSolr = mongoSetIDs.difference(solrSetIDs)
    	#объединение множеств (данные, которые есть и там, и там)
		intersectionSetForSolr = mongoSetIDs.intersection(solrSetIDs)
		#сравнение элементов
		for elem in intersectionSetForSolr:
        	#если дата не совпадает, то мы "обновляем" (т.е. удаляем и добавляем заново) запись
			if (datetime.fromtimestamp(dictSolr[elem] / 1e3)-timedelta(hours = 5) != dictMongo[elem]):
            	#добавление элемента во множество на удаление
				deleteSetForSolr.add(elem)
            	#добавление элемента во множество на добавление
				addSetForSolr.add(elem)
		#удаление элементов
		for elem in deleteSetForSolr:
        	#удаление элемента по ID из Solr
			__priorityQueue__.put({"URL": solr_url + '/' + solr_collection + '/update?commit=true', "headers":headers, "data":dumps({"delete":{"query": myConfig["fieldCommonIDMongoInSolr"] + ":"+elem}})})
		#получение элементов на добавление
		addElements = list(collection.find({myConfig["fieldIDMongo"]:{"$in":[ bson.objectid.ObjectId(i) for i in addSetForSolr]}}))
		for elem2 in addElements:
			#print(elem2)
			#добавление элементов по ID в Solr
			__priorityQueue__.put({"URL": URL, "headers": headers, "data": dumps(elem2)})
	else:
		val = collection.find()
		for v in val:
			__priorityQueue__.put({"URL": URL, "headers": headers, "data": dumps(v)})

#получение документа по запросу 
def get_documents_by_query(solr_url, solr_collection, query):
	return loads(requests.get(solr_url + '/' + solr_collection + '/select?' + query ).content)

#определение функции первого потока 
def queue_into_request():
	logger = logging.getLogger("App.queue_into_request")
    #бесконечный цикл работы потока
	while True:
		try:
        #получение первой записи из очереди
			if(not __priorityQueue__.empty()):
				data = __priorityQueue__.get()
				#отправка запроса
				render_request(data)
				logger.info("HELP")
				#Если это был последний запрос из курсора, значит можно разрешить запросы для синхронизации баз
			elif(not isSendOnlyPermissions and not __secondaryQueue__.empty()):
				#print("Secondary")
				data = __secondaryQueue__.get()
				#отправка запроса
				render_request(data)
				logger.info("PLEASE")
		except Exception as e:
			logger.error(e)

#функция добавления записи в Solr
def add_to_solr(solr_url, solr_collection, data):
    #часть URL
	path = '/update/json/docs?commit=true'
    #заголовок запроса
	headers = {'Content-type': "application/json"}
    #добавление записи в очередь
	__secondaryQueue__.put({"URL": solr_url + '/' + solr_collection + path, "headers":headers, "data":dumps(data)})

#функция удаления записи из Solr
def delete_to_solr(solr_url, solr_collection, data):
    #часть URL
	path = '/update?commit=true'
    #заголовок запроса
	headers = {'Content-type': "application/json"}
    #добавление записи в очередь на удаление
	__secondaryQueue__.put({"URL": solr_url + '/' + solr_collection + path, "headers":headers, "data":dumps({"delete":{"query": myConfig["fieldCommonIDMongoInSolr"] + ":"+str(data)}})})

#функция получения документов из Mongo по ID
def get_document_by_id(id, collection):
	#print(loads(dumps(collection.find({myConfig["fieldIDMongo"]: id})[0])))
	return collection.find({myConfig["fieldIDMongo"]: id})[0]
	
#функция отслеживания обновлений базы Mongo
def observer_collection(collection, solr_url, solr_collection):
	try:
        #бесконечный цикл для постоянной проверки данных
		while(True):
            #контекст изменения базы
			with collection.watch() as stream:
                #цикл по изменениям в базе
				for change in stream:
                    #отправка данных на определение метода обработки
					definition_methods(solr_url, solr_collection, change, collection)
                    #логирование
					logger = logging.getLogger("App.observer")
					logger.info("get by mongo collection = " + collection.name + ", data = " + str(change))
            #условие закрытия потока
			if thread_break:
				break
    #логирование при возникновении исключения
	except Exception as e:
		logger = logging.getLogger("App.observer")
		logger.error(e)

#функция для определения типа запроса к Solr
def definition_methods(solr_url, solr_collection, cursor, collection):
    #если курсор на добавление, то добавляем документ
	if cursor["operationType"] == "insert":
		add_to_solr(solr_url, solr_collection, cursor["fullDocument"])
    #если курсор на замену, то удаляем и добавляем обновленный документ
	if cursor["operationType"] == "replace":
		delete_to_solr(solr_url, solr_collection, cursor["documentKey"][myConfig["fieldIDMongo"]])
		add_to_solr(solr_url, solr_collection, cursor["fullDocument"])
    #если курсор на обновление, то удаляем и добавляем обновленный документ
	if cursor["operationType"] == "update":
		delete_to_solr(solr_url, solr_collection, cursor["documentKey"][myConfig["fieldIDMongo"]])
		add_to_solr(solr_url, solr_collection, get_document_by_id(cursor["documentKey"][myConfig["fieldIDMongo"]], collection))
    #если курсор на удаление, то удаляем документ
	if cursor["operationType"] == "delete":
		#print(cursor['documentKey'].keys())
		delete_to_solr(solr_url, solr_collection, cursor["documentKey"][myConfig["fieldIDMongo"]])

#функция по отправке данных на Solr
def render_request(data):
	logger = logging.getLogger("App.render_request")
	while True:
		try:
            #отправка данных на Solr
			r = requests.post(data["URL"], data = data["data"], headers = data["headers"])
            #определение, был ли завершён запрос или нет
			if (r.status_code != 200):
                #снова отправить запрос через 5 секунд
				logger.info("failed to send request :( " + r.url + " content: " + r.content.decode("utf-8"))
				#print("failed to send request :( " + r.url + " content: " + r.content.decode("utf-8"))
				time.sleep(5)
				continue
            #логирование отправки данных
			#logger = logging.getLogger("App.render_request")
			logger.info("send request " + r.url + ", content: " + r.content.decode("utf-8"))
			#print("send request " + r.url + ", content: " + r.content.decode("utf-8"))
			return r
		except ConnectionError as e:
			logger.error(e)
			

#создание контекста демона
context = daemon.DaemonContext(stdout=sys.stdout, stderr=sys.stderr, files_preserve = files)
#запуск контекста
with context:
	main()
