import mrop
import math
from itertools import groupby
import re


def count_documents(state, document):
    for column in state:
        state[column] += 1
    return state


graph_count_docs = mrop.ComputationalGraph(source='main_input')
graph_count_docs.name = 'count_documents_graph'
graph_count_docs.add_operation(mrop.Fold(count_documents, {'docs_count': 0}))


def split_text_length_gt_four(row):
    words = re.findall(r'[\w]+', row['text'])
    for word in words:
        if len(word) > 4:
            yield {
                'doc_id': row['doc_id'],
                'word': word.lower()
            }


graph_split_words = mrop.ComputationalGraph(source='main_input')
graph_split_words.name = 'split_text_graph'
graph_split_words.add_operation(mrop.Map(split_text_length_gt_four))


def filter_by_presence_in_two_docs(rows):
    table = list(rows)
    flag = True
    groups = {key: list(group) for key, group in
              groupby(table, lambda item: item['doc_id'])}

    docs_counter = 0

    for key in groups:
        if len(groups[key]) >= 2:
            docs_counter += 1
    if docs_counter < rows[0]['docs_count']:
        flag = False
    if flag:
        for index in range(len(rows)):
            yield {
                'word': rows[0]['word'],
                'doc_id': rows[index]['doc_id']
            }


def word_counter(rows):
    yield {
        'word': rows[0]['word'],
        'frequency of word in all docs': len(rows)
    }


graph = mrop.ComputationalGraph(source=graph_split_words)
graph.name = 'filter_words_graph'
graph.add_operation(mrop.Join(on=graph_count_docs, key=['word', 'docs_count'],
                              strategy='outer'))
graph.add_operation(mrop.Sort(key=['word', 'doc_id']))
graph.add_operation(mrop.Reduce(filter_by_presence_in_two_docs, key=['word']))
graph.add_operation(mrop.Reduce(word_counter, key=['word']))


def freq_of_word_in_doc(rows):
    yield {
        'doc_id': rows[0]['doc_id'],
        'word': rows[0]['word'],
        'frequency in doc': len(rows)
    }


def compute_pmi_select_top_ten(rows):
    for index in range(len(rows)):
        rows[index] = {
            'doc_id': rows[index]['doc_id'],
            'word': rows[index]['word'],
            'pmi': math.log(rows[index]['frequency in doc'] /
                            rows[index]['frequency of word in all docs'])
        }

    rows = sorted(rows, key=lambda item: -item['pmi'])
    top_words = [item['word'] for item in rows]
    yield {
        'doc_id': rows[0]['doc_id'],
        'top_pmi_words': top_words[:10]
    }


graph_calc_pmi = mrop.ComputationalGraph(source=graph_split_words)
graph_calc_pmi.name = 'calc_pmi_graph'
graph_calc_pmi.add_operation(mrop.Sort(['doc_id', 'word']))
graph_calc_pmi.add_operation(mrop.Reduce(freq_of_word_in_doc, ['doc_id', 'word']))
graph_calc_pmi.add_operation(mrop.Join(on=graph, key='word', strategy='left'))
graph_calc_pmi.add_operation(mrop.Sort(['doc_id']))
graph_calc_pmi.add_operation(mrop.Reduce(compute_pmi_select_top_ten, key=['doc_id']))

graph_calc_pmi.run(main_input=open('text_corpus.txt', 'r'),
                   save_result=open('pmi_output.txt', 'w'), verbose=True)
