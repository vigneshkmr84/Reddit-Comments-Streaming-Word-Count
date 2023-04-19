import spacy
from spacy import displacy

NER = spacy.load("en_core_web_sm")

raw_text="""1, 2, 3 are very good. Messi is the GOAT. India is a poor country."""

text1= NER(raw_text)

for word in text1.ents:
    print(word.text,word.label_)

    # GPE, ORG, PERSON