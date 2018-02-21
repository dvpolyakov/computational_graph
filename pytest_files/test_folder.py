import sys
sys.path.append("..")
import mrop

data = [{'doc_id': 'first_text', 'text': 'simple text'},
        {'doc_id': 'second_text', 'text': 'more words here'},
        {'doc_id': "third_text", 'text': 'Hello world'}]


def count_documents(state, document):
    state['docs_count'] += 1
    return state


folder_node = mrop.Fold(count_documents, {'docs_count': 0})
folder_node.previous_node_iter = data
result = list(iter(folder_node))


def test_res_of_folder():
    assert result == [{'docs_count': 3}]
