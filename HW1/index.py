__author__ = 'harrymao'
from os.path import dirname, basename, abspath
import glob
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
import re
path ='/Users/harrymao/Desktop/IR/HW1/AP_DATA/ap89_collection/*'
global count, filenum
count = 0
filenum=0
es = Elasticsearch()
class data:
    def __init__(self, docno, text, idt):
        self.docno = docno
        self.text  = text
        self.id = idt

    def __str__(self):
        return ("docno: "+self.docno+"text: "+self.text+"id: "+str(self.id))

def getAllFiles():
    files = []
    for filename in glob.glob(path):
        files.append(filename)
    files.remove('/Users/harrymao/Desktop/IR/HW1/AP_DATA/ap89_collection/readme')
    # for f in files:
    #     print(f)
    return files

def readFile(f):
    global filenum
    filecon = open(f,'r')
    content =  filecon.read()
    filecon.close()
    contentList = re.findall(r'<DOC>(.*?)</DOC>',content, re.DOTALL)
    filenum+=len(contentList)
    return contentList

def extractInfo(contentList):
    global count
    datalist = []
    for content in contentList:
        docno = re.search(r'<DOCNO>(.*?)</DOCNO>',content).group(1)
        texts = re.findall(r'<TEXT>(.*?)</TEXT>', content, re.DOTALL)
        combine = ''.join(texts)
        d = data(docno,combine,count)
        datalist.append(d)
        count += 1
    return datalist

def extractData():
    files = getAllFiles()
    data=[]
    for f in files:
        contents = readFile(f)
        d = extractInfo(contents)
        data.extend(d)

    return data

def indexing(data):
    doc = {
        'docno': data.docno,
        'text': data.text
    }
    es.index(index='ap_dataset', doc_type = 'document', id = data.id, body = doc)


def main():
    datalist = extractData()
    for d in datalist:
        indexing(d)
    print(filenum)
    print(len(datalist))

if __name__=='__main__':
    main()

