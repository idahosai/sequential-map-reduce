import csv
import re
import string
import time
from collections import Counter
from functools import reduce


PREPOSITIONS = set([
    'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
    'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
])


def file_to_words_Two(filename):
    """Read a file and return a sequence of word values.
    """
    STOP_WORDS = set([
        'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
        'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
    ])
    # replace all the punctuations with spaces
    TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

    # print multiprocessing.current_process().name, 'reading', filename
    output = []

    with open(filename, encoding="utf8") as f:

        reader = csv.reader(f)

        for line in reader:
            # print("current line is:", line)
            for x in line:
                if x.lstrip().startswith('..'):
                    continue

            # print("lets see:", line)
            # if list(line).lstrip().startswith('..'):  # Skip rst comment lines
            #    continue
            # store a translated copy of the string
            for x in range(len(line)):
                # line[x].translate(TR)  # Strip punctuation
                line[x] = line[x].translate(TR)

                # print("this is x:", line[x])
                # print("last see", line)
                # iterate though the lines list that is splits on all whitespace
                # for word in x.split():
                line[x] = line[x].lower()
                # check if all characters in the itterated line's word is a letter
                # as well as check if the word is a prepositions
                if line[x].isalpha() and line[x] not in STOP_WORDS:
                    # print("word added to final list:", line[x])
                    output.append(line[x])
    return output


def word_enhancer(word):
    # remove spaces and '[',']' value then make the word become all lowercase
    return re.sub(r'[^\w\s]', '', word).lower()


def stopwords(word):
    return word not in PREPOSITIONS and word and word.isalpha()


def mapper(text):
    # added this here
    # print(type(text))
    # print(text)
    # print(str(text))
    tokens_in_text = str(text).split()
    # print("after str():", type(tokens_in_text))
    # returns map object
    tokens_in_text = map(word_enhancer, tokens_in_text)
    # print("type:", type(tokens_in_text))
    # print("after clean_word():", list(tokens_in_text))
    # returns an iterator that passed the function check for each element in the iterable
    tokens_in_text = filter(stopwords, tokens_in_text)
    # do length of each member in the list & put it in a dictionary
    # print(type(tokens_in_text))
    return Counter(tokens_in_text)


def reducer(cnt1, cnt2):
    cnt1.update(cnt2)
    return cnt1


def chunk_mapper(chunk):
    mapped = map(mapper, chunk)
    # print("m:", list(mapped))
    # print("in chunk mapper:", list(mapped))
    reduced = reduce(reducer, mapped)
    # print("r:", reduced)
    return reduced


def even_chunks(list_to_chunk, number_of_chunks=2):
    for i in range(0, len(list_to_chunk), number_of_chunks):
        yield list_to_chunk[i:i + number_of_chunks]


if __name__ == "__main__":
    each_word_in_file = file_to_words_Two("Donald_Tweets.csv")
    data_chunks = list(even_chunks(each_word_in_file, number_of_chunks=5))
    # print("data chunk is:", data_chunks)
    # data_chunks = ['young', 'bong', 'dou', 'young'] #chunkify(large_list_of_strings, number_of_chunks=8)
    # step 1:
    # returns a list.apply fuction to each element in list so str in this case
    # mapped = map(mapper, data_chunks)
    t1 = time.time()
    mapped = map(chunk_mapper, data_chunks)

    # mapped should be  a list of list at this point
    # step 2:
    # extend the list into one list
    reduced = reduce(reducer, mapped)
    t2 = time.time()
    print("all the words from donald trump:", reduced)
    # print("Donald trumps most common words used:", reduced.most_common(10))
    print("serial processing took: ", t2 - t1)
