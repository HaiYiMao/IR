__author__ = 'harrymao'

from os import path
from elasticsearch import Elasticsearch
import re
import stem as st

es = Elasticsearch()
realpath = '/Users/harrymao/Desktop/IR/HW1/AP_DATA/query_desc.51-100.short.txt'
stopwordspath = '/Users/harrymao/Desktop/IR/HW1/AP_DATA/stoplist.txt'
# global queries
# queries = []
# global stopwords
# stopwords = []
# with open(stopwordspath) as sw:
#     for line in sw:
#         word = line.replace('\n', '')
#         stopwords.append(word)

class query:
    def __init__(self, num, term):
        self.no =num
        self.terms = term

    def __str__(self):
        return 'number is '+self.no+' '+'terms is '+str(self.terms)

    def setterms(self, terms):
        self.terms = terms


def readQuery():
    queries = []
    with open(realpath) as fp:
        for line in fp:
            words = line.split()
            number = words[0].replace('.', '')
            uniterm = words[4:]
            term = query(number, uniterm)
            cleanQuery(term)
            queries.append(term)
    return queries

def cleanQuery(query):
    for t in query.terms:
        if ',' in t:
            index = query.terms.index(t)
            query.terms[index] = t.replace(',', '')

    for t in query.terms:
        if '.' in t:
            index = query.terms.index(t)
            query.terms[index] = t.replace('.', '')
    for t in query.terms:
        if 'make' ==t:
            query.terms.remove("make")
        if "taken" == t:
            query.terms.remove("taken")
        if "based" ==t:
            query.terms.remove("based")
        if "identify" ==t:
            query.terms.remove("identify")
        if  "report" ==t:
            query.terms.remove("report")
        if  "used" ==t:
            query.terms.remove("used")
        if  "certain" ==t:
            query.terms.remove("certain")
        if  "type" ==t:
            query.terms.remove("type")
        if  "specified" ==t:
            query.terms.remove("specified")
        if  "basis" ==t:
            query.terms.remove("basis")
        if  "individuals" ==t:
            query.terms.remove("individuals")
        if  "organizations" ==t:
            query.terms.remove("organizations")







def removeall(list, content):
    for l in list:
        if l == content:
            list.remove(content)

# def stemming(list):
#     p = st.PorterStemmer()
#     for term in list.terms:
#         newword = p.stem(term, 0, len(term)-1)
#         listReplace(list.terms, term, newword)
#
# def listReplace(list, oldword, newword):
#     for l in list:
#         if l == oldword:
#             i = list.index(oldword)
#             list.pop(i)
#             list.insert(i, newword)

def getTerms():
    queries = readQuery()
    return queries

def indexQuery():
    queryList = getTerms()
    es = Elasticsearch()
    for element in queryList:
        query = ' '.join(element.terms)
        doc = {
                'text': query
              }
        es.index(index='queries', doc_type = 'document', id = element.no, body= doc)

def getFreq(docid):
    frequence = {}
    result= es.termvector(index="queries", doc_type="document", id=docid)
    for term in result["term_vectors"]["text"]["terms"]:
        frequence[term] = result["term_vectors"]["text"]["terms"][term]["term_freq"]
    for term in frequence:
        print "frequence of "+term+" is: "+str(frequence[term])
    return frequence

def getindexedTerms(docid):
    terms = []
    result= es.termvector(index="queries", doc_type="document", id=docid)
    for term in result["term_vectors"]["text"]["terms"]:

        terms.append(term)
    return terms

class indexedQueries():

    def __init__(self):
        self.terms=[]
        self.no = 0
        self.frequence = {}

    def __init__(self, query):
        self.no = query.no
        self.terms = getindexedTerms(self.no)
        self.frequence = getFreq(self.no)

    def tostr(self):
        print "id: "+str(self.no)
        print "terms:{0}".format(self.terms)
        for e in self.frequence:
            print "{0} frequence is {1}".format(e, self.frequence[e])

def get_indexedQueries():
    indexQuery()
    list = getTerms()
    indexed_queries = []
    for l in list:
        query = indexedQueries(l)
        indexed_queries.append(query)
    return indexed_queries

def main():
     rawlist = getTerms()
     for rl in rawlist:
         print rl
     list = get_indexedQueries()
     for l in list:
         print len(l.terms)
         print l.terms



if __name__ == '__main__':
    main()