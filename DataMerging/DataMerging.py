import gensim
import pandas as pd
import regex as re
import numpy as np
import string
import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.feature import Word2Vec
from pyspark.sql.types import *
from pyspark.ml.clustering import KMeans

class DataMerging:

    def __init__(self, vancouver_file, toronto_file):
        self.stopWords = self.get_stopwords("stopwords.txt")

        self.vandata = pd.read_csv(vancouver_file)
        self.vanBusinessType = self.vandata.BusinessType.unique()

        self.tordata = pd.read_csv(toronto_file)
        self.torBusinessType = self.tordata.BusinessType.unique()

    @staticmethod
    def tokenize(text_input, stop_words):
        """
        1. Spilt text into tokens
        2. Remove stop words
        3. Convert each token to lower case
        :param text_input: Input text
        :param stop_words: Stop words to remove
        :return:
        """
        replace_punctuation = string.maketrans(string.punctuation, ' ' * len(string.punctuation))
        text_input = text_input.translate(replace_punctuation)

        # 1. split string into list of tokens
        text_tokens = re.split(r'\W+', text_input)

        # 2 & 3 convert to lower case and remove stop words
        new_token = []
        for word in text_tokens:
            if word.lower() not in stop_words and word != "":
                new_token.append(word.lower())
        return new_token

    @staticmethod
    def get_stopwords(stopword_file):
        """
        Get stop words from file
        :param stopword_file:
        :return:
        """
        f = open(stopword_file, "r")
        return set(f.read().split("\n"))


    #implementing jaccard
    def jaccard_sim(self, a, b):
        """
        Calculates jacard similarity based on word
        :param a:
        :param b:
        :return:
        """
        a = set(a)
        b = set(b)

        if len(a) == 0 or len(b) == 0:
            return 0

        return float(len(a.intersection(b))) / (len(a.union(b)))

    def similarity_twolist_jaccard(self):
        """
        Find n^2 jaccard similarity between two lists
        Sort them in reverse order by their score
        :param business_type1:
        :param business_type2:
        :return:
        """
        similar_list = []
        for tor in self.torBusinessType:
            for van in self.vanBusinessType:
                jval = self.jaccard_sim(self.tokenize(tor, self.stopWords),
                                   self.tokenize(van, self.stopWords))
                if jval > 0.0:
                    similar_list.append((tor, van, jval))
            similar_list.sort(key=lambda tup: tup[2], reverse=True)
        return similar_list

    def similarity_twolist_w2v(self, w2vfile='./GoogleNews-vectors-negative300.bin'):
        """
        Find n^2 Google word2vec similarity between two lists
        Sort them in reverse order by their score
        :param business_type1:
        :param business_type2:
        :return:
        """
        # Load Google's pre-trained Word2Vec model.
        model = gensim.models.KeyedVectors.load_word2vec_format(w2vfile, binary=True)

        similar_list = []
        for tor in self.torBusinessType:
            for van in self.vanBusinessType:
                jval = model.wmdistance(self.tokenize(tor, self.stopWords),
                                   self.tokenize(van, self.stopWords))
                # if jval > 0.0:
                similar_list.append((tor, van, jval))
            similar_list.sort(key=lambda tup: tup[2], reverse=False)

        del model
        return similar_list

    def save_similarity_list(self, similar_list, filename, n=1000):
        """
        Save similarity list to file
        :param filename:
        :return:
        """
        f = open(filename, "w")
        csvwrite = csv.writer(f)
        csvwrite.writerow(["BusinessType1", "BusinessType2", "IsMatch(Yes/No)"])
        for d in similar_list[0:n]:
            csvwrite.writerow([d[0], d[1],""])

    def get_entity_map(self, file_src):
        """
        Get crowsourced matched entity
        from 1st business type to second
        :param file_src:
        :return: Dictionary of a entity map
        """
        f = open(file_src, "r")
        crowsource = csv.reader(f)
        map_src = dict()
        for row in crowsource:
            if row[2] == "Yes":
                if row[1] in map_src:
                    #print row[1] +"," +row[0]+","+map_src[row[1]]
                    pass
                else:
                    map_src[row[1]] = row[0]
        return map_src

    def get_businesstype_list_replace_entity(self, entity_map):
        """
        Get business type list
        :return:
        """
        map_src = self.get_entity_map(entity_map)
        for i in range(len(self.torBusinessType)):
            if self.torBusinessType[i] in map_src:
                self.torBusinessType[i] = map_src[self.torBusinessType[i]]

        btypes = set()
        for data in self.vanBusinessType:
            btypes.add(data)
        for data in self.torBusinessType:
            btypes.add(data)

        business_type_list = []
        for row in btypes:
            business_type_list.append(row)

        return business_type_list

    # similar_list = find_similarity_w2v()
    # for d in similar_list[0:1000]:
    #     print str(d[0])+","+ str(d[1])+","+ str(d[2])

    # similar_list = find_similarity_jaccard()
    # for d in similar_list[0:1000]:
    #     print str(d[0])+","+ str(d[1])+","+ str(d[2])

    def find_high_frequency_words(self):
        """
        Get high frequency words for two business types
        It will help to add stop words
        :return:
        """
        newbtype = np.append(self.vanBusinessType, self.torBusinessType)
        wordshf = dict()
        for data in newbtype:
            towords = self.tokenize(data, self.stopWords)
            for word in towords:
                if word not in wordshf:
                    wordshf[word] = 1
                else:
                    wordshf[word] += 1
        import operator
        sorted_x = sorted(wordshf.items(), key=operator.itemgetter(1), reverse=True)

        return sorted_x

    def get_cluster_map(self, file_src):
        """
        Get business name to generic business name from
        clustered and crowd-sourced
        :param file_src:
        :return:
        """
        import csv
        f = open(file_src, "r")
        clus = csv.reader(f)
        map_src = dict()
        for row in clus:
            if row[0] in map_src:
                #print row[1] +"," +row[0]
                pass
            else:
                map_src[row[0]] = row[2]
        return map_src


    # print(entity_map)
    # print(cluster_map)

    def business_tocluster(self, business_name, entity_map, cluster_map):
        """
        Get business type to generic business type
        :param business_name:
        :param entity_map:
        :param cluster_map:
        :return:
        """
        if business_name in entity_map:
            business_name = entity_map[business_name]

        if business_name in cluster_map:
            business_name = cluster_map[business_name]
        else:
            print business_name
            return ""
        return business_name


    def write_map(self, file_dest, entity_map_file, cluster_map_file):
        """
        Write Business type to  clustered name business
        :param file_dest:
        :return:
        """
        entity_map = self.get_entity_map(entity_map_file)
        cluster_map = self.get_cluster_map(cluster_map_file)

        f = open(file_dest,"w")
        csvwriter = csv.writer(f)

        for data in self.vanBusinessType:
            csvwriter.writerow([data, self.business_tocluster(data, entity_map, cluster_map)])
        for data in self.torBusinessType:
            csvwriter.writerow([data, self.business_tocluster(data, entity_map, cluster_map)])

    def read_map(self, file_src):
        """
        Read all business maped to cluster name
        :param file_src:
        :return: Dictionary to convert business name to cluster
        """
        all_business = dict()
        f = open(file_src,"r")
        csvreader = csv.reader(f)
        for csvline in csvreader:
            all_business[csvline[0]] = csvline[1]
        return all_business

    def change_business_type_save(self, data, filename, business_map):
        """
        Change type of business to generic name and save
        :param filename:
        :return:
        """
        for index, row in data.iterrows():
            try:
                data.set_value(index,col="BusinessType", value=business_map[row.BusinessType])
            except KeyError:
                print index, row
                pass

        data.to_csv(filename, quoting=True)

    def write_final_files(self, map_file, van_final_file, tor_final_file):
        """
        Write final files for vancouver and toronto
        :param van_final_file:
        :param tor_final_file:
        :return:
        """
        all_business = self.read_map(map_file)

        self.change_business_type_save(self.vandata, van_final_file, all_business)
        self.change_business_type_save(self.tordata, tor_final_file, all_business)

    def cluster_business_types(self, business_types, cluster_size, output_path):

        types = []
        for row in business_types:
            types.append(tuple((self.tokenize(row, self.stopWords), row)))


        conf = SparkConf().setAppName('Squeeze merge data')
        spark_context = SparkContext(conf=conf)
        sql_context = SQLContext(spark_context)

        schema = StructType([
            StructField("text", ArrayType(StringType())),
            StructField("business", StringType())
        ])

        # Input data: Each row is a bag of words from a sentence or document.
        documentDF = sql_context.createDataFrame(schema=schema, data=types)

        # Learn a mapping from words to Vectors.
        word2Vec = Word2Vec(vectorSize=300, minCount=0, inputCol="text",
                            outputCol="result")
        model = word2Vec.fit(documentDF)
        result = model.transform(documentDF)

        #result.show()

        # Trains a k-means model.
        kmeans = KMeans(featuresCol="result",
                        k=cluster_size, predictionCol="cluster")
        model1 = kmeans.fit(result)

        result1 = model1.transform(result)
        result1 = result1.orderBy(["cluster"])

        result1.rdd.map(lambda x:str(x[1]) + "," +str(x[3])).\
            coalesce(1).saveAsTextFile(output_path)


if __name__ == "__main__":
    ROOT_PATH = "/home/jay/BigData/SFUCourses/CMPT733/ProjectData/"

    VANCOUVER_DATA = ROOT_PATH + "Vancouver/FINAL_VANCOUVER.csv"
    TORONTO_DATA = ROOT_PATH + "Toronto/FINAL_TORONTO.csv"
    dm = DataMerging(vancouver_file=VANCOUVER_DATA,
                     toronto_file=TORONTO_DATA)

    # Step1: Find similarity between two unique business types
    similar_list = dm.similarity_twolist_jaccard()
    #similar_list = dm.similarity_twolist_w2v()
    dm.save_similarity_list(similar_list, "./ToCrowdsource-EntityMatch.csv")
    #print(similar_list)

    # Step2: Crowdsource match
    print("CROWDSOURCING Entity match:")
    print("Labelling column to if entity is match or not")
    print("Example: Taxi Driver , Cab Driver, Yes")
    print("Billiard club , Dance club, No")
    print("Outputfile: EntityMatched.csv")

    #Step3: Adapt crowsourced data for entity resolution
    merged_business_types = dm.get_businesstype_list_replace_entity("./EntityMatch.csv")
    #print merged_business_types
    dm.cluster_business_types(merged_business_types,30,output_path="./ToCrowdsource-Clustered" )
    import shutil
    shutil.move("./ToCrowdsource-Clustered/part-00000", "./ToCrowdsource-Clustered.csv")


    # Step4: Crowdsource clustername
    print("CROWDSOURCING: CLuster")
    print("Labelling column to if entity is match or not")
    print("Example: Taxi Driver , Cab Driver, Yes")
    print("Billiard club , Dance club, No")
    print("Outputfile: Clustered.csv")

    # Step5: Creating a map
    dm.write_map("./To-Crowdsource-BusinessToCluster.csv", "./EntityMatch.csv", "./Clustered.csv")

    # Step6: Clean data for duplicate rows
    print("Step6: Cleaning data")

    # Step7: creating final dataset
    dm.write_final_files("BusinessToCluster.csv", "./VANDATA_FINAL.csv","./TORDATA_FINAL.csv")







