from os import walk
from os.path import join
from src.data.doc import Doc
from xml.etree import ElementTree

from src.data.token import Token
import pickle
import spacy
import re
from tqdm import tqdm
import os


class IDataLoader(object):
    def __init__(self):
        pass

    def read_data_from_corpus_folder(self, corpus):
        raise NotImplementedError('Method should be overridden with data loader, example exb_data_loader')


class EcbDataLoader(IDataLoader):
    def __init__(self):
        super(EcbDataLoader, self).__init__()

    def read_data_from_corpus_folder(self, corpus):
        documents = list()
        for (dirpath, folders, files) in walk(corpus):
            for file in files:
                is_ecb_plus = False
                if file.endswith('.xml'):
                    print('processing file-', file)

                    if 'ecbplus' in file:
                        is_ecb_plus = True

                    tree = ElementTree.parse(join(dirpath, file))
                    root = tree.getroot()
                    doc_id = root.attrib['doc_name']
                    tokens = list()
                    doc_text = ''
                    for elem in root:
                        if elem.tag == 'token':
                            sent_id = int(elem.attrib['sentence'])
                            tok_id = elem.attrib['number']
                            tok_text = elem.text
                            if is_ecb_plus and sent_id == 0:
                                continue
                            if is_ecb_plus:
                                sent_id = sent_id - 1

                            tokens.append(Token(sent_id, int(tok_id), tok_text))
                            if doc_text == '':
                                doc_text = tok_text
                            elif tok_text in ['.', ',', '?', '!', '\'re', '\'s', 'n\'t', '\'ve',
                                              '\'m', '\'ll']:
                                doc_text += tok_text
                            else:
                                doc_text += ' ' + tok_text

                    documents.append(Doc(doc_id, doc_text, tokens))

        return documents


class TweetsDataLoader(IDataLoader):
    def __init__(self):
        super(TweetsDataLoader, self).__init__()

    def read_data_from_corpus_folder_old(self, corpus):
        nlp = spacy.load('en_core_web_sm')
        documents = list()
        data = pickle.load(open(corpus, 'rb'))
        pairs_counter = 0
        for rule, pairs in data[:2]:
            print('start reading {}'.format(rule))
            for topic in tqdm(pairs):
                for tweet in topic:
                    doc_id = '{}_{}'.format(tweet[0], pairs_counter)
                    text = tweet[1]
                    tokens = list()
                    doc_text = ''
                    doc = nlp(text)
                    for sent_id, sent in enumerate(doc.sents):
                        #  TODO: maybe change the tok_id (raise only for valid tokens)
                        for token_id, token in enumerate(sent):
                            tok_text = str(token)

                            #  ignore URL tokens
                            if tok_text in ['#', '@'] or self.is_url(tok_text):
                                continue
                            #  remove @ from tokens
                            if len(tok_text) > 1 and tok_text.startswith('@'):
                                tok_text = tok_text.replace('@', '', 1)

                            tokens.append(Token(sent_id, token_id, tok_text))
                            if doc_text == '':
                                doc_text = tok_text
                            elif tok_text in ['.', ',', '?', '!', '\'re', '\'s', 'n\'t', '\'ve',
                                              '\'m', '\'ll']:
                                doc_text += tok_text
                            else:
                                doc_text += ' ' + tok_text
                    documents.append(Doc(doc_id, doc_text, tokens))
                pairs_counter += 1
        return documents

    def read_data_from_corpus_folder(self, corpus):
        documents = list()
        data = pickle.load(open(corpus, 'rb'))
        for rule_data in data:
            pairs_counter = 0
            rule = rule_data['path']
            rule_name = os.path.basename(rule).replace('.pk', '')
            print('start reading {}'.format(rule))
            for topic in tqdm(rule_data['data']):
                for tweet in topic['tweets']:
                    doc_text = ''
                    doc_id = '{}_{}{}'.format(tweet['id'], pairs_counter, rule_name)
                    # text = tweet['text']
                    tokens = list()
                    for sent_id, sent in enumerate(tweet['tokens']):
                        #  TODO: maybe change the tok_id (raise only for valid tokens)
                        for token_id, token in enumerate(sent):
                            tok_text = token
                            tokens.append(Token(sent_id+1, token_id, tok_text))
                            if doc_text == '':
                                doc_text = tok_text
                            elif tok_text in ['.', ',', '?', '!', '\'re', '\'s', 'n\'t', '\'ve',
                                              '\'m', '\'ll']:
                                doc_text += tok_text
                            else:
                                doc_text += ' ' + tok_text

                    documents.append(Doc(doc_id, doc_text, tokens))
                pairs_counter += 1
        return documents
    @staticmethod
    def is_url(token):
        """
        check if the token is a url
        :param token: the token to check on
        :return: if the token is a URL: True, if not: False
        """
        url = re.search('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+] | [! * @ssj \(\),] | (?: %[0-9a-fA-F][0-9a-fA-F]))+',
                        token)
        return bool(url)
