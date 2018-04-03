## Gets a paragraph from a user and identifies the important words in that paragraph

from flask import Flask, render_template, request
from nltk import word_tokenize,pos_tag, RegexpParser

app = Flask(__name__)

@app.route('/')
def index():
    return render_template("importantwords.html")

@app.route('/output', methods=['POST'])
def output():
    if request.method == 'POST':
        result = request.form['inputdata']
        result = result.lower()
        #converting the paragraph in tokens for parts of speech tagging
        words = word_tokenize(result)
        allwords=[]
        for word in words:
            if word not in allwords:
                allwords.append(word)
        ##lm = WordNetLemmatizer()
        rootwords = []
        ##for word in allwords:
        ##   rootwords.append(lm.lemmatize(word))
        #tagged all the parts of speech to words
        taggedwords = pos_tag(allwords)
        #splits the string and gets favorable parts of speech tagged words... Here I have taken all Noun, adjectives, Verbs and Adverbs
        chunkString = """Junk*1234: {<.*>+}
                                }<NN*|JJ|VB.|RB>+{"""
        chunkParse = RegexpParser(chunkString)
        chunkedwords = chunkParse.parse(taggedwords)
        impwords = []
        ## coverting chunked words tree to list of favorable words
        for words in chunkedwords:
            impwords.append(str(words))
        keyvalue=[]
        for word in impwords:
            if ('Junk*1234' not in word):
                keyvalue.append(word[1:].split(",")[0])
        return render_template("output.html", keyvalues=keyvalue,result=result)

if __name__=="__main__":
    app.run(debug=True) ## starts this app... start the server
