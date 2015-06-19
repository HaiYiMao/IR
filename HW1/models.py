__author__ = 'harrymao'
import retrieveQuery as RQ
import searchValue as sv
import math
import operator


global Averange
AverageLen = sv.getAveLen()
result = RQ.getQuery()
global tfList
tfList = result[0]
global queryList
queryList = result[1]
global query_size
query_size = result[2]
global queries
queries = result[3]


class ValueTermDoc:
    def __init__(self, docno, id, term, value, df, total, tfq, qid):
        self.docno = docno
        self.id = id
        self.term = term
        self.value = value
        self.df = df
        self.total = total
        self.tfq = tfq
        self.qid = qid

    def __str__(self):
        return 'docno:'+self.docno+' id:'+str(self.id)+' term:'+self.term +' value:'+str(self.value)+' df:'+str(self.df)


class OkapiTF:
    def __init__(self):
        self.value_term = {}
        self.docLength = {}

    def getOkapiTF(self):
        global tfList
        valueList = []
        for termtf in tfList:
                id = termtf.id
                word = termtf.term
                tf = termtf.tf
                docno = termtf.docno
                df = termtf.df
                total = termtf.total
                qid = termtf.queryno
                if id not in self.docLength:
                    length = sv.getDocLen(id)
                    self.docLength[id] = length
                OkapiValue = tf/(tf+0.5+1.5+self.docLength[id]/AverageLen)
                tfq = termtf.tfq
                valueclass = ValueTermDoc(docno, id, word, OkapiValue, df, total, tfq, qid)
                valueList.append(valueclass)
        return valueList

    def getScore(self):

        query_value={}
        valueList = self.getOkapiTF()
        for element in valueList:
            if element.qid not in query_value:
                query_value[element.qid] = {}
                query_value[element.qid][element.docno] = element.value
            elif element.docno in query_value[element.qid]:
                query_value[element.qid][element.docno]+=element.value
            else :
                query_value[element.qid][element.docno] = element.value

        return query_value

    def output(self):
        global queryList
        score = self.getScore()
        file = open("result_OKAPI.txt", "w")
        for qid in queryList:
            scorequery = score[qid]
            sorted_result = sorted(scorequery.items(), key=operator.itemgetter(1), reverse=True)
            for i in range(len(sorted_result)):
               file.write("{0} Q0 {1} {2} {3} Exp\n".format(qid, sorted_result[i][0], i, sorted_result[i][1]))


global OkapiTFclass
OkapiTFclass = OkapiTF()
global OtfList
OtfList = OkapiTFclass.getOkapiTF()


class TF_IDF:

    def __init__(self):
        self.value_term = {}
        self.docLength = {}
        self.D = 84678
        #global OkapiTFclass
        #global OtfList
        #self.D = len(self.OtfList)

    def getTF_IDF(self):
        valueList = []
        global OtfList
        for element in OtfList:
            word = element.term
            id = element.id
            ovalue = element.value
            docno = element.docno
            df = element.df
            total = element.total
            qid = element.qid
            tfq = element.tfq
            tf_df = ovalue * math.log(self.D/df, 2)
            valueclass = ValueTermDoc(docno, id, word, tf_df, df, total, tfq, qid)
            valueList.append(valueclass)
        return valueList

    # def get_Score(self):
    #     doc_value={}
    #     valueList = self.getTF_IDF()
    #     for element in valueList:
    #              if element.docno not in doc_value:
    #                  doc_value[element.docno] = element.value
    #              else:
    #                  doc_value[element.docno] += element.value
    #     return doc_value
    def getScore(self):

        query_value={}
        valueList = self.getTF_IDF()
        for element in valueList:
            if element.qid not in query_value:
                query_value[element.qid] = {}
                query_value[element.qid][element.docno] = element.value
            elif element.docno in query_value[element.qid]:
                query_value[element.qid][element.docno]+=element.value
            else :
                query_value[element.qid][element.docno] = element.value

        return query_value

    def output(self):
        global queryList
        score = self.getScore()
        file = open("result_TF_IDF.txt", "w")
        for qid in queryList:
            scorequery = score[qid]
            sorted_result = sorted(scorequery.items(), key=operator.itemgetter(1), reverse=True)
            for i in range(len(sorted_result)):
                file.write("{0} Q0 {1} {2} {3} Exp\n".format(qid, sorted_result[i][0], i, sorted_result[i][1]))


class Okapi_BM25:

    def __init__(self):
        self.value_term = {}
        self.D = 84678
        #global OkapiTFclass
        #global OtfList
        #self.OtfList = self.OkapiTFclass.getOkapiTF()
        #self.AverageLen = sv.getAveLen()

    def get_BM25(self):
        global OtfList
        global tfList
        global OkapiTFclass
        global AverageLen
        valueList = []
        for element in tfList:
            word = element.term
            id = element.id
            tf = element.tf
            docno = element.docno
            df = element.df
            total = element.total
            tfq = element.tfq
            k1 = 1.5
            k2 = 1.5
            b = 0.75
            qid = element.queryno
            docLength = OkapiTFclass.docLength[id]
            bm25 = math.log((self.D+0.5)/(df+0.5), 2) * (tf+k1*tf)/(tf + k1*((1-b)+b*(docLength/AverageLen))) *(tfq+tfq*k2)/(tfq+k2)
            valueclass = ValueTermDoc(docno, id, word, bm25, df, total, tfq, qid)
            valueList.append(valueclass)
        return valueList

    # def get_Score(self):
    #      doc_value={}
    #      valueList = self.get_BM25()
    #      for element in valueList:
    #               if element.docno not in doc_value:
    #                   doc_value[element.docno] = element.value
    #               else:
    #                   doc_value[element.docno] += element.value
    #      return doc_value
    def getScore(self):

        query_value={}
        valueList = self.get_BM25()
        for element in valueList:
            if element.qid not in query_value:
                query_value[element.qid] = {}
                query_value[element.qid][element.docno] = element.value
            elif element.docno in query_value[element.qid]:
                query_value[element.qid][element.docno] += element.value
            else:
                query_value[element.qid][element.docno] = element.value

        return query_value

    def output(self):
        global queryList
        score = self.getScore()
        file = open("result_BM25.txt", "w")
        for qid in queryList:
            scorequery = score[qid]
            sorted_result = sorted(scorequery.items(), key=operator.itemgetter(1), reverse=True)
            for i in range(len(sorted_result)):
                file.write("{0} Q0 {1} {2} {3} Exp\n".format(qid, sorted_result[i][0], i, sorted_result[i][1]))


class LM_Laplace:

    def __init__(self):
        self.value_term = {}
        self.D = 84678
        #self.OkapiTFclass = OkapiTF()
        #self.OtfList = self.OkapiTFclass.getOkapiTF()
        #self.AverageLen = sv.getAveLen()
        self.V = RQ.getVValue()

    def get_LM_Laplace(self):
        global OkapiTFclass
        global OtfList
        global tfList
        valueList = []
        for element in tfList:
            word = element.term
            id = element.id
            tf = element.tf
            docno = element.docno
            df = element.df
            tfq = element.tfq
            total = element.total
            docLength = OkapiTFclass.docLength[id]
            value = (tf+1)/(docLength+self.V)
            qid = element.queryno
            valueclass = ValueTermDoc(docno, id, word, value, df, total, tfq, qid)
            valueList.append(valueclass)
        return valueList


    def getScore(self):
        global query_size
        global OkapiTFclass
        query_value={}
        valueList = self.get_LM_Laplace()
        terms_count = {}
        for element in valueList:
            if element.qid not in query_value:
                terms_count[element.qid] = {}
                query_value[element.qid] = {}
                terms_count[element.qid][(element.docno, element.id)] = 1
                query_value[element.qid][element.docno] = math.log(element.value, 2)
            elif element.docno in query_value[element.qid]:
                query_value[element.qid][element.docno] += math.log(element.value, 2)
                terms_count[element.qid][(element.docno, element.id)] += 1
            elif element.docno not in query_value[element.qid]:
                query_value[element.qid][element.docno] = math.log(element.value, 2)
                terms_count[element.qid][(element.docno, element.id)] = 1
        for queryno in query_size:
            size = query_size[queryno]
            for docno_id in terms_count[queryno]:
                if terms_count[queryno][docno_id] < size:
                    docno = docno_id[0]
                    id = docno_id[1]
                    docLength = OkapiTFclass.docLength[id]
                    miner = size - terms_count[queryno][docno_id]
                    value = miner * math.log(1.0/(docLength+self.V), 2)
                    query_value[queryno][docno] += value
                    #print ('doc:{0} miner is {2} add value {1}'.format(docno, value, miner))
        return query_value

    def output(self):
        global queryList
        score = self.getScore()
        file = open("result_Laplace.txt", "w")
        for qid in queryList:
            scorequery = score[qid]
            sorted_result = sorted(scorequery.items(), key=operator.itemgetter(1), reverse=True)
            for i in range(len(sorted_result)):
                file.write("{0} Q0 {1} {2} {3} Exp\n".format(qid, sorted_result[i][0], i, sorted_result[i][1]))

class LM_JM:

    def __init__(self):
        self.value_term = {}
        self.D = 84678
        self.V = RQ.getVValue()
        self.totalLen = {}
        self.usingList = {}
        self.baseValue = {}


    def get_fvalue(self):
        count = 0
        for v in self.OtfList:
            count += v.value
        return count

    def get_LM_JM(self):
        valueList = []
        global OkapiTFclass
        global OtfList
        global tfList

        for element in tfList:
            word = element.term
            lam = 0.99
            id = element.id
            tf = element.tf
            f_value = element.total
            docno = element.docno
            df = element.df
            tfq = element.tfq
            docLength = OkapiTFclass.docLength[id]
            qid = element.queryno
            if word not in self.totalLen:
                total = self.getTotalLen(word)
                self.totalLen[word] = total
            sigmalen = self.totalLen[word]
            value = lam*tf/docLength+(1-lam)*f_value/sigmalen
            if (docno, qid) not in self.usingList:
                self.usingList[(docno, qid)] = []
                self.usingList[(docno, qid)].append(word)
            else:
                self.usingList[(docno, qid)].append(word)
            if (word, qid) not in self.baseValue:
                self.baseValue[(word, qid)] = (1-lam)*f_value/sigmalen
            valueclass = ValueTermDoc(docno, id, word, value, df, f_value, tfq, qid)
            valueList.append(valueclass)

        return valueList

    # get the length of all documents length including given term
    def getTotalLen(self, term):
        global OtfList
        global OkapiTFclass
        count = 0
        for element in OtfList:
            if term == element.term:
                count += OkapiTFclass.docLength[element.id]
        return count


    def getScore(self):

        global queries
        global query_size
        global OkapiTFclass
        query_value={}
        valueList = self.get_LM_JM()
        terms_count = {}
        for element in valueList:
            if element.qid not in query_value:
                terms_count[element.qid] = {}
                query_value[element.qid] = {}
                terms_count[element.qid][element.docno] = 1
                query_value[element.qid][element.docno] = math.log(element.value, 2)
            elif element.docno in query_value[element.qid]:
                query_value[element.qid][element.docno] += math.log(element.value, 2)
                terms_count[element.qid][element.docno] += 1
            elif element.docno not in query_value[element.qid]:
                query_value[element.qid][element.docno] = math.log(element.value, 2)
                terms_count[element.qid][element.docno] = 1

        for queryno in query_size:
            size = query_size[queryno]
            for docno in terms_count[queryno]:
                if terms_count[queryno][docno] < size:
                    diff = set(queries[queryno]) - set(self.usingList[(docno, queryno)])
                    for d in diff:
                        base = math.log(self.baseValue[(d, queryno)])
                        query_value[queryno][docno] += base
                        print ("queryno is {0}, docno:{1}, add base value{2}".format(queryno, docno, base))
        return query_value

    def output(self):
        global queryList
        score = self.getScore()
        file = open("result_JM.txt", "w")
        for qid in queryList:
            scorequery = score[qid]
            sorted_result = sorted(scorequery.items(), key=operator.itemgetter(1), reverse=True)
            for i in range(len(sorted_result)):
                file.write("{0} Q0 {1} {2} {3} Exp\n".format(qid, sorted_result[i][0], i, sorted_result[i][1]))
def metasearch():
    metaSearchResult = {}
    global OkapiTFclass
    global queryList
    uniResult =OkapiTFclass.getScore()
    for qid in queryList:
        sorted_result = sorted(uniResult[qid].items(), key=operator.itemgetter(1), reverse=True)
        for i in range(len(sorted_result)):
            uniResult[qid][sorted_result[i][0]] = 1000-i
    tfidfclass = TF_IDF()
    result2 = tfidfclass.getScore()
    for qid in queryList:
        sorted_result = sorted(result2[qid].items(), key=operator.itemgetter(1), reverse=True)
        for i in range(len(sorted_result)):
            uniResult[qid][sorted_result[i][0]] = 1000-i
    BM25class = Okapi_BM25()
    result3 = BM25class.getScore()
    for qid in queryList:
        sorted_result = sorted(result3[qid].items(), key=operator.itemgetter(1), reverse=True)
        for i in range(len(sorted_result)):
            uniResult[qid][sorted_result[i][0]] = 1000-i
    file = open("result_Meta.txt", "w")

    for qid in queryList:
        scorequery = uniResult[qid]
        sorted_result = sorted(scorequery.items(), key=operator.itemgetter(1), reverse=True)
        for i in range(len(sorted_result)):
            file.write("{0} Q0 {1} {2} {3} Exp\n".format(qid, sorted_result[i][0], i, sorted_result[i][1]))






def main():

    # global OkapiTFclass
    # OkapiTFclass.output()
    # tfidfclass = TF_IDF()
    # tfidfclass.output()
    # BM25class = Okapi_BM25()
    # BM25class.output()
    # LM_Laplaceclass = LM_Laplace()
    # LM_Laplaceclass.output()
    # LM_JMclass = LM_JM()
    # LM_JMclass.output()
    metasearch()

if __name__ == '__main__':
    main()
