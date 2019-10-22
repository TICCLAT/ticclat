import pandas

from ticclat.flask_app import raw_queries


def paradigm_network(connection, wordform):
    xyz_df = pandas.read_sql(raw_queries.get_wxyz(), connection, params={'wordform': wordform})
    # select first result (first paradigm for wordform)
    wxyz = xyz_df.iloc[0].to_dict()
    # select the top frequent X values for same Z,Y
    # df = pandas.read_sql(raw_queries.get_frequent_x_for_zy(), connection, params={'Z': xyz['Z'], 'Y': xyz['Y']})
    df = pandas.read_sql(raw_queries.get_min_dist_x_xyz(), connection,
                         params={'Z': wxyz['Z'], 'Y': wxyz['Y'], 'X': wxyz['X']})
    x_values = df['X'].tolist()
    # append itself to the list of X values to query next
    if wxyz['X'] not in x_values:
        x_values.append(wxyz['X'])
    nodes = []
    links = []

    def flatten(nested_list):
        return [item for sublist in nested_list for item in sublist]

    nested_w = [
        pandas.read_sql(
            raw_queries.get_most_frequent_lemmas_for_xyz(),
            connection,
            params={'Z': wxyz['Z'], 'Y': wxyz['Y'], 'X': x, 'limit': 50}
        ).reset_index(drop=True).to_dict(orient='records') for x in x_values
    ]
    W_list = flatten(nested_w)
    for w in W_list:
        nodes.append({
            'id': str(w['wordform_id']),
            'tc_z': wxyz['Z'],
            'tc_y': wxyz['Y'],
            'tc_x': w['X'],
            'tc_w': w['W'],
            'type': 'w',
            'frequency': w['frequency'],
            'wordform': w['wordform']
        })
    for x in x_values:
        w_nodes_for_x = [node for node in nodes if node['tc_x'] == x]
        x_node = {
            'id': f'Z{wxyz["Z"]}Y{wxyz["Y"]}X{x}',
            'tc_z': wxyz['Z'],
            'tc_y': wxyz['Y'],
            'tc_x': x,
            'tc_w': w_nodes_for_x[0]['tc_w'],
            'type': 'x',
            'frequency': sum([node['frequency'] for node in w_nodes_for_x]),
            'wordform': w_nodes_for_x[0]['wordform']
        }
        nodes.append(x_node)

        if len(w_nodes_for_x) == 1:
            nodes.remove(w_nodes_for_x[0])
        else:
            for node in w_nodes_for_x:
                links.append({
                    'source': node['id'],
                    'target': x_node['id'],
                    'id': node['id'] + x_node['id'],
                    'type': 'XW'
                })
    root_node = list(filter(lambda node: node['tc_x'] == wxyz['X'] and node['type'] == 'x', nodes))[0]
    root_node['wordform'] = wordform
    root_node['tc_w'] = wxyz['W']
    other_x_nodes = filter(lambda node: node['type'] == 'x' and node['tc_x'] != wxyz['X'], nodes)
    for node in other_x_nodes:
        links.append({
            'source': node['id'],
            'target': root_node['id'],
            'id': node['id'] + root_node['id'],
            'type': 'XX'
        })
    return {
        'nodes': nodes,
        'links': links,
    }
