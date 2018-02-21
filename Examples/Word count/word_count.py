import mrop
import re


def split_text(row):
    words = re.findall(r'[\w]+', row['text'])
    for word in words:
        yield {
            'doc_id': row['doc_id'],
            'word': word.lower()
        }


def word_counter(rows):
    yield {
        'word': rows[0]['word'],
        'number': len(rows)
    }


graph = mrop.ComputationalGraph(source='main_input')
graph.name = 'count_words_graph'
graph.add_operation(mrop.Map(split_text))
graph.add_operation(mrop.Sort(key=['word']))
graph.add_operation(mrop.Reduce(word_counter, key=['word']))

graph.run(main_input=open('text_corpus.txt', 'r'),
          save_result=open('word_count_output.txt', 'w'))
