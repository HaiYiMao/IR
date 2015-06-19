__author__ = 'harrymao'
import getTerms as gt
from elasticsearch import Elasticsearch


class tfValue:
    def __init__(self, no, term, tf, id, df, total, tfq, queryno):
        self.docno = no
        self.term = term
        self.tf = tf
        self.id = id
        self.df = df
        self.tfq = tfq
        self.total = total
        self.queryno = queryno

    def __str__(self):
        return 'docno is '+self.docno+'term is '+self.term+' tf value is '+str(self.tf)+'id is '+str(self.id)


class docValue:

    def __init__(self, docno,id):
        self.docno = docno
        self.id = id
        self.tfValueAll = {}

    def addtfValue(self, value, term):
        self.tfValueAll[term] = str(value)

    def to_str(self):
        print 'id is:'+self.id
        for term in self.tfValueAll:
            print(term+':'+self.tfValueAll[term]+' ')



def getQuery():

    queryList = []
    tfvalueList = []
    querySize = {}
    queryterms = {}
    es = Elasticsearch()
    queries = gt.get_indexedQueries()

    for query in queries:
        queryno = query.no
        querySize[queryno] = len(query.terms)
        queryList.append(queryno)
        queryterms[queryno] = query.terms
        for term in query.terms:
            res = es.search(index="ap_dataset", body={
                "query": {
                    "function_score": {
                        "query": {
                            "term": {
                                "text": term
                            }
                        },
                        "functions": [
                            {
                                "script_score": {
                                    "lang": "groovy",
                                    "script_file": "tf-score",
                                    "params": {
                                        "term": term,
                                        "field": "text"
                                    }
                                }
                            }
                        ],
                        "boost_mode": "replace"
                    }
                },
                "size": 100000,
            })

            total = 0
            tempList = []
            for hit in res['hits']['hits']:
                docno = hit['_source']['docno'].encode('UTF-8')
                tf = hit['_score']
                if tf == 0:
                    print "term: {0}, docno: {1}, tf:{2}".format(term, docno, tf)
                df = res['hits']['total']
                id = hit['_id']
                total += tf
                tfq = query.frequence[term]
                tfclass = tfValue(docno, term, tf, id, df, 0, tfq, queryno)
                tempList.append(tfclass)
            for l in tempList:
                l.total = total
            tfvalueList.extend(tempList)


    return tfvalueList, queryList, querySize, queryterms


def getDocno(id):
    es = Elasticsearch()
    result = es.search(
        index="ap_dataset",
        body={
            'query': {
                'filtered': {
                    'query': {
                        "match_all": {}
                    },
                    'filter': {
                        "ids": {
                            "values": [
                                int(id)
                            ]
                        }
                    }
                }
            },
            "fields": ["docno"]
        }
    )
    return str(result['hits']['hits'][0]['fields']['docno'][0])

def getVValue():
    es = Elasticsearch()
    res = es.search(
        index="ap_dataset",
        body={
                "aggs":{
                    "unique_terms":{
                        "cardinality": {
                            "script": "doc['text']"
                        }
                    }

                }
        }
    )
    Vvalue = res["aggregations"]["unique_terms"]["value"]
    return Vvalue


def main():
    list = getQuery()[0]
    for l in list:
        if l.total == 0:
            print "doc:{0}, total:{1}, ".format(l.docno, l.total)


if __name__ == '__main__':
    main()
