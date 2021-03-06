from kneed import KneeLocator
from sklearn.cluster import DBSCAN, AgglomerativeClustering
from sklearn.neighbors import NearestNeighbors
from sklearn.decomposition import PCA
import math
import pandas as pd
import numpy as np
from .tokenization import Tokens
import editdistance
from .phraser import phraser
from .sequence_matching import Match
import re


class MLClustering:

    def __init__(self, df, groups, tokens, vectors, cpu_number, add_placeholder, method):
        self.groups = groups
        self.df = df
        self.method = method
        self.tokens = tokens
        self.vectors = vectors
        self.distances = None
        self.epsilon = None
        self.min_samples = 1
        self.cpu_number = cpu_number
        self.add_placeholder = add_placeholder


    def process(self):
        if self.method == 'dbscan':
            return self.dbscan()
        if self.method == 'hdbscan':
            return self.hdbscan()
        if self.method == 'hierarchical':
            return self.hierarchical()


    def dimensionality_reduction(self):
        #n = self.vectors.detect_embedding_size(self.tokens.vocabulary_pattern)
        n = self.vectors.detect_embedding_size(self.tokens.get_vocabulary(self.groups['sequence']))
        print('Number of dimensions is {}'.format(n))
        pca = PCA(n_components=n, svd_solver='full')
        pca.fit(self.vectors.sent2vec)
        return pca.transform(self.vectors.sent2vec)


    def kneighbors(self):
        """
        Calculates average distances for k-nearest neighbors
        :return:
        """
        k = round(math.sqrt(len(self.vectors.sent2vec)))
        nbrs = NearestNeighbors(n_neighbors=k, n_jobs=-1).fit(self.vectors.sent2vec)
        distances, indices = nbrs.kneighbors(self.vectors.sent2vec)
        self.distances = [np.mean(d) for d in np.sort(distances, axis=0)]


    def epsilon_search(self):
        """
        Search epsilon for the DBSCAN clusterization
        :return:
        """
        kneedle = KneeLocator(self.distances, list(range(len(self.distances))))
        self.epsilon = max(kneedle.all_elbows) if (len(kneedle.all_elbows) > 0) else 1


    def dbscan(self):
        """
        Execution of the DBSCAN clusterization algorithm.
        Returns cluster labels
        :return:
        """
        self.vectors.sent2vec = self.vectors.sent2vec if self.vectors.w2v_size <= 10 else self.dimensionality_reduction()
        self.kneighbors()
        self.epsilon_search()
        self.cluster_labels = DBSCAN(eps=self.epsilon,
                                     min_samples=self.min_samples,
                                     n_jobs=self.cpu_number) \
            .fit_predict(self.vectors.sent2vec)
        self.groups['cluster'] = self.cluster_labels
        print('DBSCAN finished with {} clusters'.format(len(set(self.cluster_labels))))
        return pd.DataFrame.from_dict(
            [item for item in self.groups.groupby('cluster').apply(func=self.gb_regroup)],
            orient='columns').sort_values(by=['cluster_size'], ascending=False)



    def hdbscan(self):
        import hdbscan
        self.vectors.sent2vec = self.vectors.sent2vec if self.vectors.w2v_size <= 10 else self.dimensionality_reduction()

        clusterer = hdbscan.HDBSCAN(min_cluster_size=100, min_samples=1)
        self.cluster_labels = clusterer.fit_predict(self.vectors.sent2vec)
        self.groups['cluster'] = self.cluster_labels
        print('HDBSCAN finished with {} clusters'.format(len(set(self.cluster_labels))))
        return pd.DataFrame.from_dict(
            [item for item in self.groups.groupby('cluster').apply(func=self.gb_regroup)],
            orient='columns').sort_values(by=['cluster_size'], ascending=False)



    def hierarchical(self):
        """
        Agglomerative clusterization
        :return:
        """
        self.tokens.sent2vec = self.vectors.sent2vec if self.vectors.w2v_size <= 10 else self.dimensionality_reduction()
        self.cluster_labels = AgglomerativeClustering(n_clusters=None,
                                                      distance_threshold=0.1) \
            .fit_predict(self.tokens.sent2vec)
        self.groups['cluster'] = self.cluster_labels
        self.result = pd.DataFrame.from_dict(
            [item for item in self.groups.groupby('cluster').apply(func=self.gb_regroup)],
            orient='columns').sort_values(by=['cluster_size'], ascending=False)


    def gb_regroup(self, gb):
        # Search for the common tokenized pattern
        pattern_matcher = Match(gb['tokenized_pattern'].values)
        tokenized_pattern = pattern_matcher.sequence_matcher(self.add_placeholder)
        # and detokenize it to common tectual pattern
        pattern = Tokens.detokenize_row(Tokens.TOKENIZER, tokenized_pattern)
        pattern = re.sub('\((.*?)\)+[\S\s]*\((.*?)\)+', '(.*?)', pattern)
        # Search for the common sequence
        matcher_sequence = Match(gb['sequence'].values)
        sequence = matcher_sequence.sequence_matcher(False)
        # Generate text from all group sequences
        text = '. '.join([' '.join(row) for row in gb['sequence'].values])
        # Extract common phrases
        phrases = phraser(text)
        # Get all indices for the group
        indices = [i for sublist in gb['indices'].values for i in sublist]
        size = len(indices)
        return {'pattern': pattern,
                'sequence': sequence,
                'tokenized_pattern': tokenized_pattern,
                'indices': indices,
                'cluster_size': size,
                'common_phrases': phrases.extract_common_phrases()}



    def levenshtein_similarity(self, top, rows):
        """
        Search similarities between top and all other sequences of tokens.
        May be used for strings as well.
        top - most frequent sequence
        rows - all sequences
        :param rows:
        :return:
        """
        if len(rows) > 1:
            return (
                [(1 - editdistance.eval(top, rows[i]) / max(len(top), len(rows[i]))) for i in
                 range(0, len(rows))])
        else:
            return [1]
