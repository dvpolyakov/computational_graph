import sys
sys.path.append("..")
import mrop


table = [{'doc_id': 'first_text', 'word': 'я считаю'},
         {'doc_id': 'first_text', 'word': 'я считаю'},
         {'doc_id': 'first_text', 'word': 'что это саботаж'},
         {'doc_id': 'first_text', 'word': 'что это саботаж'}]


def unique(rows):
    yield rows[0]


reducer_node = mrop.Reduce(unique, ['word'])
reducer_node.previous_node_iter = table
result = list(elem for elem in iter(reducer_node))
print(result)

expected_result = [{'doc_id': 'first_text', 'word': 'я считаю'},
                   {'doc_id': 'first_text', 'word': 'что это саботаж'}]


def test_res_of_reducer():
    assert result == expected_result
