import sys
import re
sys.path.append("..")
import mrop


def split_text(row):
    words = re.findall(r'[\w]+', row['text'])
    for word in words:
        yield {
            'doc_id': row['doc_id'],
            'word': word.lower()
        }


data = [{'doc_id': 'first_text', 'text': 'simple text is written here'}]


mapper_node = mrop.Map(split_text)
mapper_node.previous_node_iter = data
result = list(iter(mapper_node))

expected_result = [{'doc_id': 'first_text', 'word': 'simple'},
                   {'doc_id': 'first_text', 'word': 'text'},
                   {'doc_id': 'first_text', 'word': 'is'},
                   {'doc_id': 'first_text', 'word': 'written'},
                   {'doc_id': 'first_text', 'word': 'here'}]


def test_result_of_folder():
    assert result == expected_result
