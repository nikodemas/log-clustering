import math
import numpy as np
from string import punctuation
from nltk.corpus import stopwords
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
from nltk.corpus import words


class TermsAnalysis:

    def __init__(self, tokenized_cluster, tokenized_pattern):

        self.tokenized_cluster = tokenized_cluster
        self.tokenized_pattern = tokenized_pattern


    def process(self):

        dct = Dictionary(self.tokenized_cluster)
        corpus = [dct.doc2bow(line) for line in self.tokenized_cluster]
        tfidf = TfidfModel(corpus)
        corpus_tfidf = tfidf[corpus]
        d = {dct.get(id): value for doc in corpus_tfidf for id, value in doc}
        weights = [value for doc in corpus_tfidf for id, value in doc]
        unique_weights = np.unique(weights)
        percentile_10 = round(len(unique_weights)*0.02)
        top = np.sort(unique_weights)[-percentile_10:][0]

        new_arr = []
        for i,row in enumerate(self.tokenized_cluster):
            new_arr.append([self.tokenized_pattern[i][j] if ((item in d and d[item] <= top) or (item in punctuation) or (item not in d)
                                     or (item.lower() in set(stopwords.words('english') or (item.lower() in words.words('english')))))
                            else '｟*｠' for j,item in enumerate(row)])
        return new_arr

        # f_matrix = self.create_frequency_matrix(self.tokenized)
        # tf_matrix = self.create_tf_matrix(f_matrix)
        # dpw = self.create_documents_per_words(tf_matrix)
        # idf_matrix = self.create_idf_matrix(tf_matrix, dpw, len(self.tokenized))
        # tf_idf = self.create_tf_idf_matrix(tf_matrix, idf_matrix)
        # return self.remove_unnecessary(self.tokenized, tf_idf)



    def create_frequency_matrix(self, tokenized):
        frequency_matrix = []

        for tokens in tokenized:
            freq_table = {}
            for token in tokens:
                if token in freq_table:
                    freq_table[token] += 1
                else:
                    freq_table[token] = 1

            frequency_matrix.append(freq_table)

        return frequency_matrix


    def create_tf_matrix(self, freq_matrix):
        tf_matrix = []

        for f_table in freq_matrix:
            tf_table = {}

            count_words_in_sentence = len(f_table)
            for word, count in f_table.items():
                tf_table[word] = count / count_words_in_sentence

            tf_matrix.append(tf_table)

        return tf_matrix


    def create_documents_per_words(self, freq_matrix):
        word_per_doc_table = {}

        for f_table in freq_matrix:
            for word, count in f_table.items():
                if word in word_per_doc_table:
                    word_per_doc_table[word] += 1
                else:
                    word_per_doc_table[word] = 1

        return word_per_doc_table


    def create_idf_matrix(self, freq_matrix, count_doc_per_words, total_documents):
        idf_matrix = []

        for f_table in freq_matrix:
            idf_table = {}

            for word in f_table.keys():
                idf_table[word] = math.log10(total_documents / float(count_doc_per_words[word]))

            idf_matrix.append(idf_table)

        return idf_matrix


    def create_tf_idf_matrix(self, tf_matrix, idf_matrix):
        tf_idf_matrix = []

        for f_table1, f_table2 in zip(tf_matrix, idf_matrix):

            tf_idf_table = {}

            for (word1, value1), (word2, value2) in zip(f_table1.items(),
                                                        f_table2.items()):  # here, keys are the same in both the table
                tf_idf_table[word1] = float(value1 * value2)

            tf_idf_matrix.append(tf_idf_table)

        return tf_idf_matrix


    def remove_unnecessary(self, tokenized, tf_idf):
        new_arr = []
        for i, row in enumerate(tokenized):
            print(row)
            weights = [tf_idf[i][token] for token in row]
            print(weights)
            top = np.mean(weights) + np.std(weights)
            stopset = set(stopwords.words('english'))
            new_arr.append([v if weights[x] < top or v in punctuation or v in stopset
                            else '｟*｠' for x,v in enumerate(row)])
            print([v if weights[x] < top or v in punctuation else '｟*｠' for x,v in enumerate(row)])
            # print([v if weights[x] < top else '｟*｠' for x,v in enumerate(row)])
        return new_arr


    # def clean_tokens(self, tokenized):
    #     """
    #     Clean tokens from english stop words, numbers and punctuation
    #     :return:
    #     """
    #     stop = stopwords.words('english') + list(punctuation) + ["``", "''"] + words.words('english')
    #     result = []
    #     for row in tokenized:
    #         tokenized = []
    #         for i in row:
    #             if i.lower() not in stop:
    #                 tokenized.append(i)
    #         result.append(tokenized)
    #     return result