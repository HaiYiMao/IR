__author__ = 'harrymao'
from elasticsearch import Elasticsearch
es = Elasticsearch()

def getDocLen(doc_id):
    count = 0
    res = es.search(
        index="ap_dataset",
        doc_type='document',
        body={
            "query": {
                "match": {
                    "_id": doc_id
                }
            },
            "aggs": {
                "avg_docs": {
                    "stats": {
                        "script": "doc['text'].values.size()"
                    }
                }
            }
        })
    count = res["aggregations"]["avg_docs"]['sum']

    return count

def getAveLen():
    count = 0
    res = es.search(
        index="ap_dataset",
        doc_type='document',
        body={
            "query": {
                "match_all": {
                 }

                },
            "aggs": {
                "avg_docs": {
                    "stats": {
                        "script": "doc['text'].values.size()"
                    }
                }
            }
        })

    count=res["aggregations"]["avg_docs"]['avg']
    return count







# def averageLen():
#     dist = getAllLen()
#     total = 0
#     for i in dist:
#         =+dist[i]



def main():
    print getAveLen()

if __name__ == '__main__':
    main()
