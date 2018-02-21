import sys
sys.path.append("..")
import mrop

data = [{'doc_id': 'first_text', 'text': 'b'},
        {'doc_id': 'second_text', 'text': 'c'},
        {'doc_id': "third_text", 'text': 'a'}]


def count_documents(state, document):
    for column in state:
        state[column] += 1
    return state


sorter_node = mrop.Sort(key=['text'], table=data)
result = list(iter(sorter_node))


def test_result_of_sorter():
    assert result == [{'doc_id': "third_text", 'text': 'a'},
                      {'doc_id': 'first_text', 'text': 'b'},
                      {'doc_id': 'second_text', 'text': 'c'}]
