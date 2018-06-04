#!/usr/bin/python
import requests, json
import mimetypes
import os,os.path
import urllib2
import urlparse
import shutil
import time
import threading
import multiprocessing
import Queue
import socket
from itertools import cycle


#set options to logger
import logging
logging.basicConfig(format = u'[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level = logging.DEBUG)

def make_bound_socket(source_ip):
    def bound_socket(*a, **k):
        sock = true_socket(*a, **k)
        sock.bind((source_ip, 0))
        return sock
    return bound_socket

def filename_from_url(url):
    return os.path.basename(urlparse.urlsplit(url)[2])

def download_file(url):
    """Create an urllib2 request and return the request plus some useful info"""
    name = filename_from_url(url)
    
    if '192.168.23.222' in url or '62.76.90.20' in url:
        hostIp = ''
    else:
        hostIp = next(IpListC)
    #hostIp=''
        
    socket.socket = make_bound_socket(hostIp)
    r = urllib2.urlopen(urllib2.Request(url))
    info = r.info()
    
    if 'Content-Disposition' in info:
        # If the response has Content-Disposition, we take filename from it
        name = info['Content-Disposition'].split('filename=')[1]
        if name[0] == '"' or name[0] == "'":
            name = name[1:-1]
    elif r.geturl() != url:
        # if we were redirected, take the filename from the final url
        name = filename_from_url(r.geturl())
    content_type = None
    if 'Content-Type' in info:
        content_type = info['Content-Type'].split(';')[0]
    # Try to guess missing info
    if not name and not content_type:
        name = 'unknown'
    elif not name:
        name = 'unknown' + mimetypes.guess_extension(content_type) or ''
    elif not content_type:
        content_type = mimetypes.guess_type(name)[0]
    return r, name, content_type

def download_file_locally(url, dest,filename):
    req, filenameURL, content_type = download_file(url)
    extension = mimetypes.guess_extension(content_type, strict=False)
    filename = filename + extension
    if dest.endswith('/'):
        dest = os.path.join(dest , filename)
    with open(dest, 'wb') as f:
        shutil.copyfileobj(req, f)
    req.close()
    return filename, content_type

def download_task():
    while True:
        try:
	    fileId=None
	    print 'Queue size: ' + str(q.qsize())
            rJSON = q.get();
	    print (rJSON)
            fileId = rJSON['data']['file_id']
            fileName = rJSON['data']['file_name']
            filePath = externalPath+fileName
            url = rJSON['data']['url'].encode('utf8')
            fileNameNew, content_type = download_file_locally(url,externalPath,fileName)
            if os.path.isfile(externalPath+fileNameNew):
                logging.info('file downloaded')
                JSONData = {'token':token, 'file_id':fileId,'file_name':fileNameNew,'file_size':os.path.getsize(externalPath+fileNameNew),'file_type':content_type,'external_path':'/video'}
                logging.info(JSONData)
                r = requests.post(urlSetFile, data=JSONData)
            else:
                logging.error('didn\'t download')
                JSONData = {'token':token, 'file_id':fileId}
                logging.error(JSONData)
                r = requests.post(urlSetFileDownloadProblem, data=JSONData)
        except Exception as e:
	    logging.error('didn\'t download' + str(e))
            JSONData = {'token':token, 'url': url, 'file_id' : fileId }

            try:
                r = requests.post(urlSetFileDownloadProblem, data=JSONData)
            except Exception as message:
                logging.error(message)

def downloader():
    JSONGetTask = {'token':token, 'server_ident':serverIdent}
    while True:
        try:
            r = requests.post(urlGetTask, data=JSONGetTask)
            rJSON = r.json()
	    logging.info(rJSON)
            if rJSON['data'] != 'end' and rJSON['data']['file_id'] and rJSON['data']['url']:
                 logging.info('add task')
                 q.put(rJSON)
            else:
                time.sleep(5)
        except Exception as e:
	
            logging.error('didn\'t get task')
            logging.error(str(e))

            try:
                r = requests.post(urlSetFileDownloadProblem, data=JSONData)
            except Exception as message:
                logging.error(message)
            finally:
		print 'sleep30'
                time.sleep(30)
                
true_socket = socket.socket

IpListC = cycle([
    '168.01.01.01',
    '168.01.01.02'])

token = 'tokenString'

serverIdent = 'source.origin.site.ru'
externalPath = '/data/video/'

urlSite = "http://system.site.ru"

urlGetTask = urlSite + "/storage/getTask"
urlSetFile = urlSite + "/storage/setFile"
urlSetFileDownloadProblem = urlSite + "/storage/setFileDownloadProblem"
cpus=multiprocessing.cpu_count() #detect number of cores
q = Queue.Queue(cpus*2)
logging.info(cpus*2)
for i in range(cpus):
     t = threading.Thread(target=download_task)
     t.daemon = True
     t.start()

downloader()
