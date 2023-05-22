"""Catalogue CDS forms to OGC API Processes compliant inputs translators."""

# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any


def translate_string_list(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema: dict[str, Any] = {"type": "array", "items": {"type": "string"}}
    input_ogc_schema["items"]["enum"] = sorted(input_cds_schema["details"]["values"])
    return input_ogc_schema


def extract_groups_values(
    groups: list[Any], values: list[Any] | None = None
) -> list[Any]:
    if values is None:
        values = []

    for group in groups:
        if "values" in group:
            values.extend(group["values"])
        elif "groups" in group:
            values = extract_groups_values(group["groups"], values)
    return values


def translate_string_list_array(
    input_cds_schema: dict[str, Any],
) -> dict[str, Any]:
    values = extract_groups_values(input_cds_schema["details"]["groups"])
    input_ogc_schema: dict[str, Any] = {
        "type": "array",
        "items": {"type": "string", "enum": sorted(list(set(values)))},
    }
    return input_ogc_schema


def translate_string_choice(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema = {
        "type": "string",
        "enum": input_cds_schema["details"]["values"],
        "default": input_cds_schema["details"].get("default", None),
    }
    return input_ogc_schema


def translate_geographic_extent_map(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema = {
        "type": "array",
        "minItems": 4,
        "maxItems": 4,
        "items": {"type": "number"},
        "default": input_cds_schema["details"].get("default", None),
    }
    return input_ogc_schema


SCHEMA_TRANSLATORS = {
    "StringListWidget": translate_string_list,
    "StringListArrayWidget": translate_string_list_array,
    "StringChoiceWidget": translate_string_choice,
    "GeographicExtentWidget": translate_geographic_extent_map,
}


def make_ogc_input_schema(cds_input_schema: dict[str, Any]) -> dict[str, Any]:
    cds_input_type = cds_input_schema["type"]
    ogc_input_schema = SCHEMA_TRANSLATORS[cds_input_type](cds_input_schema)
    return ogc_input_schema


def translate_cds_form(
    cds_form: list[Any] | dict[str, Any],
) -> dict[str, Any]:
    """Translate CDS forms inputs into OGC API compliants ones.

    Convert inputs information contained in the provided CDS form file
    into a Python object compatible with the OGC API - Processes standard.

    Parameters
    ----------
    cds_form : list[Any] | dict[str, Any]
        CDS form.

    Returns
    -------
    dict[str, Any]
        Python object containing translated inputs information.
    """
    if not isinstance(cds_form, list):
        cds_form = list(cds_form)
    ogc_inputs = {}
    for cds_input_schema in cds_form:
        if cds_input_schema["type"] in SCHEMA_TRANSLATORS:
            ogc_inputs[cds_input_schema["name"]] = {
                "title": cds_input_schema["label"],
                "schema_": {**make_ogc_input_schema(cds_input_schema)},
            }

    return ogc_inputs
