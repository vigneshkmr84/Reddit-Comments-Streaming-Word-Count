import nltk

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
text = "Barack Obama was born in Hawaii."

tokens = nltk.word_tokenize(text)
tagged = nltk.pos_tag(tokens)
entities = nltk.chunk.ne_chunk(tagged)

print(entities)