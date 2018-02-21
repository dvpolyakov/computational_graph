import mrop
import math
import re


def count_documents(state, document):
    for column in state:
        state[column] += 1
    return state


graph_count_docs = mrop.ComputationalGraph(source='main_input')
graph_count_docs.name = 'joining graph'
graph_count_docs.add_operation(mrop.Fold(count_documents, {'docs_count': 0}))


def split_text_with_doc_id(row):
    words = re.findall(r'[\w]+', row['text'])
    for word in words:
        yield {
            'doc_id': row['doc_id'],
            'word': word.lower()
        }


def unique(rows):
    yield rows[0]


def docs_with_particular_word_counter(rows):
    yield {
        'word': rows[0]['word'],
        'docs_where_word_is_present': len(rows),
        'docs_count': rows[0]['docs_count']
    }


def calc_idf(row):
    yield {
        'word': row['word'],
        'idf': math.log(row['docs_count']/row['docs_where_word_is_present'])
    }


graph_split_words = mrop.ComputationalGraph(source='main_input')
graph_split_words.name = 'read file graph'
graph_split_words.add_operation(mrop.Map(split_text_with_doc_id))


graph_count_idf = mrop.ComputationalGraph(source=graph_split_words)
graph_count_idf.name = 'count idf graph'
graph_count_idf.add_operation(mrop.Sort(['doc_id', 'word']))
graph_count_idf.add_operation(mrop.Reduce(unique, ['doc_id', 'word']))
graph_count_idf.add_operation(mrop.Join(on=graph_count_docs, key = ['word', 'docs_count'], strategy='outer'))
graph_count_idf.add_operation(mrop.Sort(['word']))
graph_count_idf.add_operation(mrop.Reduce(docs_with_particular_word_counter, ['word']))
graph_count_idf.add_operation(mrop.Map(calc_idf))


def freq_of_word_in_doc(rows):
    yield {
        'doc_id': rows[0]['doc_id'],
        'word': rows[0]['word'],
        'frequency': len(rows)
    }


def compute_tf_idf_select_top_three(rows):
    for index in range(len(rows)):
        rows[index] = {
            'doc_id': rows[index]['doc_id'],
            'word': rows[index]['word'],
            'tf-idf': rows[index]['frequency']*rows[index]['idf']
        }
    rows = sorted(rows, key=lambda item: -item['tf-idf'])
    top_docs = tuple(item['doc_id'] for item in rows[:3])
    top_tf_idf = tuple(item['tf-idf'] for item in rows[:3])

    index = list(zip(top_docs, top_tf_idf))

    yield {
        "word": rows[0]['word'],
        'index': index
    }


graph_calc_index = mrop.ComputationalGraph(source=graph_split_words)
graph_calc_index.name = 'calc_index'
graph_calc_index.add_operation(mrop.Sort(['doc_id', 'word']))
graph_calc_index.add_operation(mrop.Reduce(freq_of_word_in_doc, ['doc_id', 'word']))
graph_calc_index.add_operation(mrop.Join(on=graph_count_idf, key='word', strategy='left'))
graph_calc_index.add_operation(mrop.Sort(['word']))
graph_calc_index.add_operation(mrop.Reduce(compute_tf_idf_select_top_three, key=['word']))

graph_calc_index.run(main_input=open('text_corpus.txt', 'r'),
                     save_result=open('tf_idf_output.txt', 'w'))


