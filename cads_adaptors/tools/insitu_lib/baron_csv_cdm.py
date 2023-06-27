import requests
import time
import pandas as pd


def clean_name(inp):
    return inp.lower().replace(' ', '_')


def names_conversion(vname, source_def, out2in=False):
    if out2in:
        for v in source_def['descriptions']:
            if clean_name(source_def['descriptions'][v]['name_for_output']) == vname:
                return v
    else:
        try:
            return clean_name(source_def['descriptions'][vname]['name_for_output'])
        except Exception as err:
            print(err)
    return None


def get_mandatory_columns(source_def, out_names=True):
    if out_names:
        return [
            names_conversion(name, source_def, out2in=False)
            for name in source_def['mandatory_columns']
        ]
    else:
        return source_def['mandatory_columns']


def cdm_converter(
        csv_file,
        source,
        dataset=None,
        end_point='obs-base-ref',
        out_file=None,
        include_missing=False
):
    #
    # the csv file has names for output.
    # we have mandatory columns that constitute the "index" and they all are defined
    #
    _sd_url = f'http://{end_point}/api/{dataset}/service_definition'

    # print(_sd_url)
    service_definition = requests.get(_sd_url).json()
    # print(service_definition)
    source_def = service_definition['sources'][source]

    out_file = f'{source}_{int(time.time())}.csv' if out_file is None else out_file
    _df = pd.read_csv(csv_file, comment='#', nrows=1).astype(str)
    i_cols = list(_df.columns)
    # db_cols = [names_conversion(name, source_def, out2in=True) for name in i_cols]
    mandatory_columns = get_mandatory_columns(source_def)
    mandatory_columns = [_ for _ in service_definition['out_columns_order'] if _ in mandatory_columns]

    variables = [v for v in i_cols if v not in mandatory_columns]
    just_variables_db = [_['columns'] for _ in source_def['products'] if _['group_name'] == 'variables'][0]
    just_variables = [source_def['descriptions'][_]['name_for_output'] for _ in just_variables_db]
    simple_variables = []
    variables_products = {}

    products = []

    ph = service_definition['products_hierarchy'][1:]

    if len(service_definition['products_hierarchy']) > 1:
        applies_to = set()
        for v in variables:
            if v in just_variables:
                # this works for any product variable
                simple_variables.append(v)
                next
            for _p in ph:
                for _sv in just_variables:
                    if v == f'{_sv}_{_p}':
                        sv = v.replace('_' + _p, '')
                        applies_to.add(_p)
                        if sv in variables_products:
                            variables_products[_sv].append(v)
                        else:
                            variables_products[_sv] = [v]
                        break
        products = [_p for _p in ph if _p in applies_to]
    else:
        simple_variables = variables

    out_columns = mandatory_columns + ['observed_variable', 'observed_value'] + products

    repacking = {}
    # print(f'{variables},\n {simple_variables}, \n {variables_products}')
    for v in simple_variables:
        repacking[v] = [i_cols.index(v)] + [i_cols.index(f'{v}_{_p}') if f'{v}_{_p}' in i_cols else False for _p in products]
    mandatory_indexes = [i_cols.index(x) for x in mandatory_columns]

    nrows = 100000
    t0 = time.time()
    wt = 0.

    def _safe(_inp: str):
        if ',' not in _inp:
            return _inp
        #print('safe not for ', _inp)
        if (_inp.startswith('"') and _inp.endswith('"')) or (_inp.startswith("'") and _inp.endswith("'")):
            return _inp
        return f'"{_inp}"'

    with open(out_file, 'w') as f:
        f.write(','.join(out_columns) + '\n')
        for df in pd.read_csv(csv_file, comment='#', chunksize=nrows, converters=dict([(ic, str) for ic in i_cols])):
            buffer = ''
            # print(df)
            for i in range(len(df.index)):
                a_row = df.values[i, :].squeeze()
                index_string = ','.join([_safe(_) for _ in a_row[mandatory_indexes]])
                # print(index_string)
                for v in repacking:
                    if a_row[repacking[v][0]] not in ('', None) or include_missing:
                        buffer += ','.join([index_string] + [v] + [a_row[row_idx] if row_idx else ''
                                                                   for row_idx in repacking[v]]) + '\n'
            _t0 = time.time()
            f.write(buffer)
            wt += time.time() - _t0
    #print(f'parsing df in {time.time() - t0} s\n time spent in writing to file {wt}')

    return out_file


def baron_cdm_converter(csv_file, source='IGRA_H', out_file=None):
    service_definition = requests.get('http://obs-base-ref/api/service_definition').json()
    source_def = service_definition['sources'][source]

    out_file = f'{source}_{int(time.time())}.csv' if out_file is None else out_file
    t0 = time.time()
    df = pd.read_csv(csv_file, comment='#', nrows=1).astype(str)
    i_cols = list(df.columns)
    db_cols = [names_conversion(name, source_def, out2in=True) for name in i_cols]

    coordinates = ['longitude', 'latitude', 'air_pressure']
    coordinates += ['air_pressure_total_uncertainty'] if 'air_pressure_total_uncertainty' in df.columns else []
    default_keys = ['long_name', 'description', 'units', 'name_for_output']
    if source in ('IGRA', 'IGRA_H'):
         index_columns = ["station_name", "report_timestamp", "report_id", "height_of_station_above_sea_level"] + coordinates
    else:
        index_columns = ["station_name", "report_timestamp", "time_since_launch", "report_id", "height_of_station_above_sea_level"] + coordinates

    db_index = [names_conversion(i, source_def=source_def, out2in=True) for i in index_columns + source_def['mandatory_columns']]

    work_on = set(db_cols).difference(
        set(db_index)
    )

    extras = set()
    print(work_on)
    for k in work_on:
        extras.update(set(source_def['descriptions'][k].keys()).difference(set(default_keys)))
    applies_to = {}

    leftover = work_on
    '''set(source_def['descriptions'].keys()).difference(
        set(source_def['mandatory_columns'] + [names_conversion(v, source_def, out2in=True) for v in coordinates])
    )'''

    for k, v in applies_to.items():
        for av in v:
            leftover.discard(source_def['descriptions'][av][k])
    print('leftover', leftover)

    print('extras', extras)
    for x in extras:
        applies_to[x] = [
            v for v in leftover if x in source_def['descriptions'][v]
        ]
    extras = list(extras)
    vnames = dict(
        [
            (ac, clean_name(source_def['descriptions'][ac]['name_for_output']))
            for ac in leftover
        ]
    )

    xnames = dict(
        [(ex, dict(
          [
              (av, source_def['descriptions'][source_def['descriptions'][av][ex]]['name_for_output'].lower().replace(' ', '_'))
              for av in applies_to[ex] if av in leftover
          ])) for ex in extras
        ]
    )

    vnames = dict([(n, vnames[n]) for n in vnames if all([ex not in vnames[n] for ex in extras])])

    out_columns = index_columns + \
                  ['observed_variable', 'observed_value'] + \
                  [_p for _p in service_definition['products_hierarchy'][1:] if _p in applies_to]
    repacking = {}

    # reduce xnames to what is present:
    tmp = {}
    remaining_names = [names_conversion(l, source_def, out2in=False) for l in work_on]
    print('remaining_names', remaining_names)
    for ex in xnames:
        if any([ex in col for col in remaining_names]):
            tmp.update({ex: xnames[ex]})
        else:
            out_columns.remove(ex)
    xnames = tmp
    print('reduced xnames', xnames)
    for v in vnames:
        #inp_name = names_conversion(v, source_def, out2in=True)

        try:
            repacking[v] = [i_cols.index(vnames[v])]
        except Exception as err:
            print("ERR", err)
            continue

        for ex in service_definition['products_hierarchy'][1:]:
            if ex in xnames and ex not in v:
                try:
                    repacking[v].append(
                        i_cols.index(xnames[ex][v]) if v in xnames[ex] and xnames[ex][v] in i_cols else False
                    )
                except Exception as err:
                    print('error',err,v, applies_to, xnames[ex])
                    raise
    print('repacking', repacking)

    mandatory_indexes = [i_cols.index(x) for x in index_columns]
    nrows = 100000
    t0 = time.time()
    wt = 0.
    with open(out_file, 'w') as f:
        f.write(','.join(out_columns) + '\n')
        for df in pd.read_csv(csv_file, comment='#', chunksize=nrows, converters=dict([(ic,str) for ic in i_cols])):
            buffer = ''
            for i in range(len(df.index)):
                a_row = df.values[i, :].squeeze()
                index_string = ','.join(a_row[mandatory_indexes])
                for v in repacking:
                    buffer += ','.join([index_string] + [vnames[v]] + [a_row[row_idx] if row_idx else ''
                                                                       for row_idx in repacking[v]]) + '\n'
            _t0 = time.time()
            f.write(buffer)
            wt += time.time() - _t0
    print(f'parsing df in {time.time()-t0} s\n time spent in writing to file {wt}')

    return out_file


def fold_df(df, ):
    pass

