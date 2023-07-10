import time

import pandas as pd
import requests


def clean_name(inp):
    return inp.lower().replace(" ", "_")


def names_conversion(vname, source_def, out2in=False):
    if out2in:
        for v in source_def["descriptions"]:
            if clean_name(source_def["descriptions"][v]["name_for_output"]) == vname:
                return v
    else:
        try:
            return clean_name(source_def["descriptions"][vname]["name_for_output"])
        except Exception as err:
            print(err)
    return None


def get_mandatory_columns(source_def, out_names=True):
    if out_names:
        return [
            names_conversion(name, source_def, out2in=False)
            for name in source_def["mandatory_columns"]
        ]
    else:
        return source_def["mandatory_columns"]


def cdm_converter(
    csv_file,
    source,
    dataset=None,
    end_point="obs-insitu",
    out_file=None,
    include_missing=False,
):
    #
    # the csv file has names for output.
    # we have mandatory columns that constitute the "index" and they all are defined
    #
    _sd_url = f"http://{end_point}/api/{dataset}/service_definition"

    # print(_sd_url)
    service_definition = requests.get(_sd_url).json()
    # print(service_definition)
    source_def = service_definition["sources"][source]

    out_file = f"{source}_{int(time.time())}.csv" if out_file is None else out_file
    _df = pd.read_csv(csv_file, comment="#", nrows=1).astype(str)
    i_cols = list(_df.columns)
    # db_cols = [names_conversion(name, source_def, out2in=True) for name in i_cols]
    mandatory_columns = get_mandatory_columns(source_def)
    mandatory_columns = [
        _ for _ in service_definition["out_columns_order"] if _ in mandatory_columns
    ]

    variables = [v for v in i_cols if v not in mandatory_columns]
    just_variables_db = [
        _["columns"] for _ in source_def["products"] if _["group_name"] == "variables"
    ][0]
    just_variables = [
        source_def["descriptions"][_]["name_for_output"] for _ in just_variables_db
    ]
    simple_variables = []
    variables_products = {}

    products = []

    ph = service_definition["products_hierarchy"][1:]

    if len(service_definition["products_hierarchy"]) > 1:
        applies_to = set()
        for v in variables:
            if v in just_variables:
                # this works for any product variable
                simple_variables.append(v)
                next
            for _p in ph:
                for _sv in just_variables:
                    if v == f"{_sv}_{_p}":
                        sv = v.replace("_" + _p, "")
                        applies_to.add(_p)
                        if sv in variables_products:
                            variables_products[_sv].append(v)
                        else:
                            variables_products[_sv] = [v]
                        break
        products = [_p for _p in ph if _p in applies_to]
    else:
        simple_variables = variables

    out_columns = mandatory_columns + ["observed_variable", "observed_value"] + products

    repacking = {}
    # print(f'{variables},\n {simple_variables}, \n {variables_products}')
    for v in simple_variables:
        repacking[v] = [i_cols.index(v)] + [
            i_cols.index(f"{v}_{_p}") if f"{v}_{_p}" in i_cols else False
            for _p in products
        ]
    mandatory_indexes = [i_cols.index(x) for x in mandatory_columns]

    nrows = 100000
    time.time()
    wt = 0.0

    def _safe(_inp: str):
        if "," not in _inp:
            return _inp
        # print('safe not for ', _inp)
        if (_inp.startswith('"') and _inp.endswith('"')) or (
            _inp.startswith("'") and _inp.endswith("'")
        ):
            return _inp
        return f'"{_inp}"'

    with open(out_file, "w") as f:
        f.write(",".join(out_columns) + "\n")
        for df in pd.read_csv(
            csv_file,
            comment="#",
            chunksize=nrows,
            converters=dict([(ic, str) for ic in i_cols]),
        ):
            buffer = ""
            # print(df)
            for i in range(len(df.index)):
                a_row = df.values[i, :].squeeze()
                index_string = ",".join([_safe(_) for _ in a_row[mandatory_indexes]])
                # print(index_string)
                for v in repacking:
                    if a_row[repacking[v][0]] not in ("", None) or include_missing:
                        buffer += (
                            ",".join(
                                [index_string]
                                + [v]
                                + [
                                    a_row[row_idx] if row_idx else ""
                                    for row_idx in repacking[v]
                                ]
                            )
                            + "\n"
                        )
            _t0 = time.time()
            f.write(buffer)
            wt += time.time() - _t0
    # print(f'parsing df in {time.time() - t0} s\n time spent in writing to file {wt}')

    return out_file
