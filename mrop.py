import json
from operator import itemgetter
from itertools import groupby


class ComputationalGraph(object):
    """
    :attribute list_of_operations (list of ComputationalGraph objects):
    list of operations (Map, Sort, Reduce, Fold or Join) in the instance
    of ComputationalGraph class (instance is linear graph).

    :attribute result (list of dicts): result table after applying whole
    chain of operations.

    :attribute dependencies (list of ComputationalGraph objects):
    list of dependencies of an instance from other graphs.
    The logic of appending another graphs to instance's dependencies
    provides the next property: for each pair of graphs graph_A and
    graph_B if graph_A is earlier in instance's dependencies list than
    graph_B, than the instance require the result of graph_A earlier,
    than result of graph_B.

    Instance may be dependent from another graph in case of:
        - result from another graph should be joined to instance;
        - result of another graph is used as an input for instance;
    In both cases graph from list of dependencies of instance should be
    run earlier than an instance.
    Topological sorting method (runs from final graph automatically)
    guarantees that all dependencies (graphs from list dependencies)
    of an instance will be run and computed earlier than an dependent
    instance.
    Topological sorting is used to avoid deep recursion calls of many
    dependencies.

    :attribute sorted_graphs (list of ComputationalGraph objects):
    topological order of graphs. Result of topological_sorting method.

    :attribute is_final_graph (bool): bool flag.
    Shows that instance should
    write its result to output file. False by default. Flag changes to
    True automatically if method .run was called by user in __main__
    file (see run method below).

    :attribute colour (str): colour of an instance ('white', 'gray' or
    'black');
    Colour of graph is used in topological sorting.


    NOTE: no iterators between linear graphs. Nodes (operations)
    connected with each other only inside one linear graph.
    """

    def __init__(self, source):
        """
        Create new line graph object
        :param source (file object of ComputationalGraph object): source
        of data for instance of class.
        Source might be file or output of another graph.
        Example of code:
            graph = mrop.ComputationalGraph(source = graph_split_words),
            where graph_split_words is an instance of an
            ComputationalGraph class;
            OR
            graph = mrop.ComputationalGraph(source='main_input'), where
            'main_input' will be specified in final_graph.run command;
        """
        self.source = source
        self.list_of_operations = []
        self.result = []
        self.dependencies = []

        if isinstance(self.source, ComputationalGraph):
            self.dependencies.append(self.source)

        self.sorted_graphs = []
        self.is_final_graph = False
        self.color = 'white'

    def run(self, **kwargs):
        """
        Run final graph by user. This command does next things:
        - performs topological sorting of graphs to know the order of
        graph's runs;
        - runs graphs one by one according to the order in sorted_graphs
        (the run_graph method will be described below);

        :attribute global_cache (dict): contains results of computation
        for each linear graph. Initialized once in final graph. Global
        cache is used to avoid unnecessary storage of data. Global cache
        mainly used in InputDataNodes to effectively use data from files
        many times.
        global_cache dict is passed to .run_graph method for all graphs.

        :attribute file_to_save_result (file object): file to save
        result from final graph;
        :attribute is_final_graph (bool): True for final graph
        (it changes automatically if .run was called by user for the
        instance in __main__ file).

        :attribute dict_of_input_files (dict): dictionary of input
        files. It's used to specify source files for graphs.

        :param kwargs (dict): dict with input files, output files and
        verbose.

        Example of code:
        graph_calc_pmi.run(main_input=open('text_corpus.txt', 'r'),
                   save_result=open('pmi_output.txt', 'w'))
        """
        self.global_cache = {}
        if 'verbose' in kwargs:
            if kwargs['verbose'] is True:
                self.global_cache['verbose'] = True
        else:
            self.global_cache['verbose'] = False
        self.file_to_save_result = kwargs['save_result']
        self.is_final_graph = True
        self.dict_of_input_files = kwargs

        # dependencies of graph will be executed first

        if self.global_cache['verbose']:
            print("Topological sorting was started")
        self.topological_sorting(self.sorted_graphs)
        if self.global_cache['verbose']:
            print("Topological sorting was successfully finished")

        if self.global_cache['verbose']:
            print("topological order is:")
            for graph in self.sorted_graphs:
                try:
                    print(graph.name)
                except AttributeError as e:
                    print("Please give names to all graphs if you want to see"
                          "the topological order")

        for graph in self.sorted_graphs:
            graph.run_graph(self.global_cache, self.dict_of_input_files)

    def topological_sorting(self, sorted_graphs: list):
        """
        Perform topological sorting (recursive DFS). Linear graph is
        used as a node.
        Directed edges appear in case of dependencies (e.g. if
        graph_first should be joined to graph_second, than there will be
        an directed edge from graph_first to graph_second.

        :param sorted_graphs (list of ComputationalGraph objects, empty
        at start);
        :return: sorted_graphs: topological order of graphs;
        """

        self.color = 'grey'
        for graph in self.dependencies:
            if graph.color == 'grey':
                raise TypeError("Cycle was detected!")
            if graph.color == 'white':
                graph.topological_sorting(sorted_graphs)
        self.color = 'black'
        sorted_graphs.append(self)

    def run_graph(self, global_cache, dict_of_input_files):
        """
        Run instance of ComputationalGraph for instances in sorted_graphs
        Performs
        - setting of source of data;
        - linear graph compilation (see below);
        - linear graph computation (see below);
        - if instance is final graph, than write result to output file;

        :param global_cache (dict);
        :param dict_of_input_files (dict);
        """
        self.verbose = global_cache['verbose']

        if self.verbose:
            if isinstance(self.source, ComputationalGraph):
                print("Source for {} is {}".format(self.name, self.source.name))

        self.list_of_operations.insert(0, InputDataNode(self.source))
        if self.verbose:
            print("run {}".format(self.name))
            print("source for {} is {}".format(self.name, self.source))

        if isinstance(self.source, str):
            self.list_of_operations[0].input_file = \
                dict_of_input_files[self.source]
        elif isinstance(self.source, ComputationalGraph):
            self.list_of_operations[0].result = self.source.result

        self.compile_graph()

        if self.verbose:
            print("list of operations for {} is {}".
                  format(self.name, self.list_of_operations))

        self.compute_graph(global_cache)

        if self.verbose:
            print("{} was successfully computed".format(self.name))

        # if isinstance(self.list_of_operations, InputDataNode):
        #     if self.list_of_operations[0].source not in global_cache:
        #         global_cache[self.list_of_operations[0].source] = \
        #             self.list_of_operations[0].file_data

        if self.is_final_graph:
            if self.verbose:
                print('writing output result from final graph to output file')
            for line in self.result:
                self.file_to_save_result.write(json.dumps(line) + '\n')
            self.file_to_save_result.close()

    def compile_graph(self):
        """
        Connect operations (nodes) in linear graph.
        For all operations in instance's list_of_operations get iterator
        from previous node to next node.
        """
        if self.verbose:
            print('starting to compile {}'.format(self.name))
            if len(self.dependencies) > 0:
                print("dependencies of {} are:".format(self.name))
                for dependency in self.dependencies:
                    try:
                        print(dependency.name)
                    except AttributeError as e:
                        print(
                            "Please give names to all graphs if you want to see"
                            "the names of dependencies")
            else:
                print("{} has no dependencies".format(self.name))
        for index in range(len(self.list_of_operations)):
            if index == 0:
                self.previous_node = iter(self.list_of_operations[0])
                continue

            self.list_of_operations[index]. \
                set_iter_from_previous_node(self.previous_node)

            self.previous_node = iter(self.list_of_operations[index])
        if self.verbose:
            print("{} was successfully compiled".format(self.name))

    def compute_graph(self, global_cache):
        """
        Compute result of linear graph using list comprehensions.
        Using iterator to previous node the method unpacks iterator to
        graph's result.

        :param global_cache;
        """
        self.list_of_operations[0].global_cache = global_cache
        self.result = list(self.list_of_operations[-1])

    def add_operation(self, new_operation):
        """
        Add operation to instance's list_of_operations.
        Instance is a linear graph.
        If new operation is Join, add Join.op to instance.dependencies.

        :param new_operation (BasicOperation's child object): and
        instance of new operation (Map, Sort, Reduce, Fold or Join);
        :return: list_of_operations in a linear graph;
        """
        if isinstance(new_operation, Join):
            self.dependencies.append(new_operation.on)
        self.list_of_operations.append(new_operation)


class BasicOperation(object):
    """
    Parent class for Map, Reduce, Sort, Fold and Join operations.

    Each inherited operation will have a method
    set_iter_from_previous_node. This method is used on a stage of graph
    compiling to connect nodes with each other.
    """

    def __init__(self):
        super().__init__()

    def set_iter_from_previous_node(self, previous_node_iter):
        """
        Takes iterator on a result from previous node.
        :param previous_node_iter (iterator object): iterator on a
        result of previous node;
        """
        self.previous_node_iter = previous_node_iter


class Map(BasicOperation):
    """
    Apply mapper (generator object) to each row
    which is output from previous node. Returns non negative number of
    rows for each input row.
    """

    def __init__(self, mapper):
        """
        :param mapper: generator object
        """
        self.mapper = mapper
        super().__init__()

    def __iter__(self):
        """
        generator delegation to mapper generator
        :return: iterator object, result of mapper
        """
        for row in self.previous_node_iter:
            yield from self.mapper(row)


class Fold(BasicOperation):
    """
    Folds table to one row using binary associative operation
    """
    def __init__(self, folder, initial_state=None):
        """
        :param folder: performs binary associative operation
        :param initial_state: init.state for folder
        :type folder: generator object
        :type initial_state: dict; e.g. {'docs_count':0}
        """
        if initial_state is None:
            raise TypeError("please specify initial state as a dict")
        self.folder = folder
        self.state = initial_state
        super().__init__()

    def __iter__(self):
        """
        generator delegation to folder
        :return: list iterator (dict): row of table, result of folder
        generator;
        """
        for row in self.previous_node_iter:
            self.state = (self.folder(self.state, row))
        yield self.state


class Sort(BasicOperation):
    """
    Sort table lexicographically

    :attribute keys_to_compare (list of strings): keys to compare rows
    by (one or more keys)
    :attribute previous_node_iter (iterator object): iterator from
    previous node.
    previous_node_iter will be used to create table to sort
    Note: is Sort was called from Join, than previous_node_iter is
    list of dicts, not an iterator.

    :attribute is_sorted (bool): flag that input table was already
    sorted;
    :attribute table (list of dicts): table to sort; comes from previous
    node by iterator;
    or as a list of dicts from Join;
    """

    def __init__(self, key, table=None):
        """
        :param key: keys to compare rows by
        :param table: option for calling Sort from Join. If Sort called
        not during Join operation, table to sort comes from previous node
        :type key: list of strings; e.g. key = ['word', 'doc_id']
        :type table (deafult None): list of dicts
        """
        if table is not None:  # Sort was called from Join
            self.previous_node_iter = table
        self.keys_to_compare = key
        self.is_sorted = False
        super().__init__()

    def __iter__(self):
        """
        Sort table once by keys and return an iterator or rows in table
        :return: iterator on result;
        """
        if not self.is_sorted:
            self.table = list(self.previous_node_iter)
            self.table = sorted(self.table,
                                key=itemgetter(*(self.keys_to_compare)))
            self.is_sorted = True
        for row in self.table:
            yield row


class Reduce(BasicOperation):
    """
    Group rows in table by keys and put group to reducer
    """

    def __init__(self, reducer, key):
        """
        :param reducer: process rows with the same keys
        :param key: keys to group rows by
        :type reducer: generator object
        :type key: list of strings; e.g. if key = ['word'], than Reduce
        will group rows with the same value row['word']

        :attribute buffer: buffer to group rows with the same value by key
        :attribute previous_node (deafult None): we need it to start to
        group rows by key
        """
        self.reducer = reducer
        if isinstance(key, list):
            self.keys_to_group_by = key
        else:
            raise TypeError("key parameter to group by in reducer should"
                            " be a list")
        self.buffer = []
        self.previous_row = None
        super().__init__()

    def __iter__(self):
        """
        generator delegation to reducer
        :return: one row (dict) for each set of rows, grouped by key

        Note: to use Reduce operation effectively (O(n)) one should sort
        input table by the same set of keys
        """
        for row in self.previous_node_iter:
            if self.previous_row is not None:
                flag = True
                for key in self.keys_to_group_by:
                    if row[key] != self.previous_row[key]:
                        flag = False
                if not flag:
                    yield from self.reducer(self.buffer)
                    self.buffer = []
            self.buffer.append(row)
            self.previous_row = row
        yield from self.reducer(self.buffer)  # to reduce last group


class Join(BasicOperation):
    """
    Analogue of JOIN operation in SQL

    Joins tables from two graphs according to strategy, selected by user
    """

    def __init__(self, on, strategy, key=None):
        """
        Set parameters of Join operation.

        :param on (ComputationalGraph object): another graph, source of
        table to be joined with;
        :param key (['key1, 'key2']): keys which two rows will be compare by;
        :param strategy (str): strategy of joining (similar with SQL syntax);
        Variants: outer (cross), left, right;
        """
        self.on = on
        if key is None:
            self.key1 = key
            self.key2 = key
        elif isinstance(key, str):
            self.key1 = key
            self.key2 = key
        else:
            self.key1 = key[0]
            self.key2 = key[1]
        self.strategy = strategy
        self.result = []
        self.is_sorted = False
        self.was_called = False
        super().__init__()

    def __iter__(self):
        """
        Generator delegation to another generator accordingly selected
        operator.
        :return: iterator object on resulting table;
        """
        if not self.is_sorted:
            self.left_table = (
                list(Sort(key=[self.key1],
                          table=list(self.previous_node_iter)))
            )
            self.right_table = (list(Sort(key=[self.key2],
                                          table=list(self.on.result))))
            self.is_sorted = True
        if self.strategy == 'outer':
            yield from self.cross(self.left_table, self.right_table)
        elif self.strategy == "left":
            yield from self.left()
        elif self.strategy == "right":
            self.key1, self.key2 = self.key2, self.key1
            self.left_table, self.right_table = \
                self.right_table, self.left_table
            yield from self.left()
        else:
            raise KeyError("please specify correct strategy of Join "
                           " operation: outer, left, right")

    def cross(self, left_group, right_group):
        """
        Implements cross join (aka SQL)
        :param left_group (list of dicts): data from previous node from
        linear graph;

        :param right_group (list of dicts): data from joined graph;
        :return: iterator object on a joined table;
        """
        for left_row in left_group:
            for right_row in right_group:
                left_row_copy = left_row.copy()
                left_row_copy.update(right_row)
                yield left_row_copy

    def left(self):
        """
        Implements LEFT OUTER JOIN (aka SQL)

        This method groups rows in left table by key1, right table by key2
        and performs FULL OUTER JOIN of these two groups.
        :return: iterator on joined table;
        """
        self.left_groups = {key: list(group) for key, group in
                            groupby(self.left_table,
                                    lambda item: item[self.key1])}
        self.right_groups = {key: list(group) for key, group in
                             groupby(self.right_table,
                                     lambda item: item[self.key2])}
        for key, left_group in self.left_groups.items():
            if key in self.right_groups.keys():
                yield from self.cross(left_group, self.right_groups[key])


class InputDataNode(BasicOperation):
    """
    Node which provide data to next operations.

    Source for InputDataNode is file or another graph.
    Loads data from file to global cache or takes result of another graph.

    Linear graph always starts from InputDataNode

    If input file was already read by another InputDataNode, than
    instance take input data from global cache.

    :attribute source: file or another graph
    :attribute result: load result of another graph if another graph is
    source of data for the new linear graph.
    """

    def __init__(self, source):
        """
        :param source (file object of ComputationalGraph object): source
        of data for InputDataNode (and the rest of linear graph);
        Source might be file or output of another graph. open file or
        another graph.
        """
        self.source = source
        self.result = []
        super().__init__()

    def __iter__(self):
        """
        Provide iterator object on input data for the rest of linear graph.
        :return: iterator object on an input table;
        """
        if isinstance(self.source, str):
            if not self.input_file.closed:
                file_data = []
                for line in self.input_file:
                    if len(line) > 2:
                        file_data.append(json.loads(str(line.strip())))

                self.global_cache[self.source] = file_data
                self.input_file.close()
            for row in self.global_cache[self.source]:
                yield row
        else:
            for row in self.result:  # get result from another graph
                yield row
