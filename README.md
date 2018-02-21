# Computational Graph
*Description of idea*: Library provides a comfortable way of calculations over tables.

# Description of concept

A table is a sequence of of dict-like objects (actually dictionaries or, for example,
json-lines of the defining dictionaries), where each dictionary is a table row. 
For simplicity, we will assume that in the table in each line all the cells, in each order the same set of keys.

Using this library, one can start the calculations by creating ComputationalGraph objects.

In a graph you can set a chain of operations which will be performed over the table. So, the data will flow through the graph (or many graphs).

The key feature of the library is that user can create the structure and order of operations separetely from running graph. 
This means that user can copy this graph structure to many servers and parallelize calculations.


Here is an exapmle how to solve word count task usin computational graph paradigm.

First step is to split text in each row. So, the first step is to apply Map operation. For each row Map operation will create many rows: one for each word in text.
Then one should add Reduce operation to graph and group rows by key 'word'. To do Reduce operation effectively, it's strongly recommended to do Sort operation on the same keys as in Reduce before Reduce.



```python
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

```     
 
You can find more examples in folder Examples.
