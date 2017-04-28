import gensim

# Load Google's pre-trained Word2Vec model.
model = gensim.models.KeyedVectors.load_word2vec_format('./GoogleNews-vectors-negative300.bin', binary=True)
print model.wmdistance(["Adult", "Industry"], ["Sex", "Industry"])
print model.wmdistance(["Adult", "Industry"], ["Office", "Store"])
print(model.similarity('france', 'spain'))