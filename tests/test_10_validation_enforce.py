import copy
import json
import os
import re
from typing import Any

import jsonschema
import jsonschema_specifications
import packaging.version
import pytest

from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.validation import enforce

MARS_STYLE_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "patternProperties": {
            "^(?!area$).+$": {  # Matches any non-null string other than "area"
                "type": "array",
                "minItems": 1,
                "items": {"type": "string", "minLength": 1},
            }
        },
        "properties": {
            "area": {
                "type": "array",
                # Note that if draft-202012 is used, "items" should be
                # renamed "prefixItems"
                "items": [
                    {
                        "type": "number",
                        "_splitOn": "/",
                        "minimum": -90.0,
                        "maximum": 90.0,
                    },
                    {"type": "number", "_splitOn": "/"},
                    {
                        "type": "number",
                        "_splitOn": "/",
                        "minimum": -90.0,
                        "maximum": 90.0,
                    },
                    {"type": "number", "_splitOn": "/"},
                ],
                "minItems": 4,
                "maxItems": 4,
            },
            "date": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "string",
                    "format": "date",
                    "_allowedFormats": ["%Y%m%d"],
                },
            },
        },
        "additionalProperties": False,  # Prevents null-string keys
    },
}

ACTUAL_MARS_SCHEMA = {
    "type": "array",
    "minItems": 1,
    "items": {
        "type": "object",
        "minProperties": 1,
        "patternProperties": {
            "^[^ ]+$": {  # Matches any non-null string that doesn't
                # contain spaces
                "type": "array",
                "minItems": 1,
                "items": {"type": "string", "minLength": 1},
            }
        },
        "additionalProperties": False,  # Prevents null-string keys
    },
}

TEST_CASES: list[dict[str, Any]] = [
    {"id": "Null test", "specs": ({}, {}, {}, None)},
    {"id": "Simplest non-null test", "specs": ({"a": "1"}, {}, {"a": "1"}, None)},
    {
        "id": "Conversion of scalar request to one-element list",
        "specs": ({"a": "1"}, {"type": "array"}, [{"a": "1"}], None),
    },
    {
        "id": "Conversion of one-element list request to scalar",
        "specs": ([{"a": "1"}], {"type": "object"}, {"a": "1"}, None),
    },
    {
        "id": "Basic no-op check with properties",
        "specs": (
            [{"a": "1"}],
            {"type": "object", "properties": {"a": {"type": "string"}}},
            {"a": "1"},
            None,
        ),
    },
    {
        "id": "Dict value scalar-to-list",
        "specs": (
            [{"a": "1"}],
            {"type": "object", "properties": {"a": {"type": "array"}}},
            {"a": ["1"]},
            None,
        ),
    },
    {
        "id": "Dict value list-to-scalar",
        "specs": (
            {"a": ["1"]},
            {"type": "object", "properties": {"a": {"type": "string"}}},
            {"a": "1"},
            None,
        ),
    },
    {
        "id": "List-to-scalar failure",
        "specs": (
            [{"a": 1}, {"a": 1}],
            {"type": "object"},
            pytest.raises(InvalidRequest),
            ("request: [{'a': 1}, {'a': 1}] is not of type 'dict'",),
        ),
    },
    {
        "id": "Error message when cannot cast list to scalar",
        "specs": (
            {"a": ["1", "2"]},
            {"type": "object", "properties": {"a": {"type": "string"}}},
            pytest.raises(InvalidRequest),
            ("request['a']: ['1', '2'] is not of type 'string'",),
        ),
    },
    {
        "id": "String to number 1",
        "specs": (
            {"a": "1"},
            {"type": "object", "properties": {"a": {"type": "integer"}}},
            {"a": 1},
            None,
        ),
    },
    {
        "id": "String to number 2",
        "specs": (
            {"a": "1.5"},
            {"type": "object", "properties": {"a": {"type": "integer"}}},
            pytest.raises(InvalidRequest),
            ("request['a']: '1.5' is not a valid integer",),
        ),
    },
    {
        "id": "String to number 3",
        "specs": (
            {"a": "1.5"},
            {"type": "object", "properties": {"a": {"type": "number"}}},
            {"a": 1.5},
            None,
        ),
    },
    {
        "id": "Error when can't convert string to number",
        "specs": (
            {"a": {"bb": [{"cc": "1.5x"}]}},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "object",
                        "properties": {
                            "bb": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {"cc": {"type": "number"}},
                                },
                            }
                        },
                    }
                },
            },
            pytest.raises(InvalidRequest),
            ("request['a']['bb'][0]['cc']: '1.5x' is not a valid number",),
        ),
    },
    {
        "id": "Integer float treated as valid int?",
        "specs": (
            {"a": 1.0},
            {"type": "object", "properties": {"a": {"type": "integer"}}},
            {"a": 1.0},
            None,
        ),
    },
    {
        "id": "Error when can't convert float to int",
        "specs": (
            {"a": 1.1},
            {"type": "object", "properties": {"a": {"type": "integer"}}},
            pytest.raises(InvalidRequest),
            ("request['a']: 1.1 is not a valid integer",),
        ),
    },
    {
        "id": "Number to string 1",
        "specs": (
            {"a": 1},
            {"type": "object", "properties": {"a": {"type": "string"}}},
            {"a": "1"},
            None,
        ),
    },
    {
        "id": "Number to string 2",
        "specs": (
            {"a": 1.0},
            {"type": "object", "properties": {"a": {"type": "string"}}},
            {"a": "1.0"},
            None,
        ),
    },
    {
        "id": "Compound example of number<->string and scalar<->list conversions",
        "specs": (
            {"a": [1], "b": ["2"], "c": 3, "d": "4"},
            {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "a": {"type": "string"},
                        "b": {"type": "integer"},
                        "c": {"type": "array", "items": {"type": "string"}},
                        "d": {"type": "array", "items": {"type": "number"}},
                    },
                },
            },
            [{"a": "1", "b": 2, "c": ["3"], "d": [4]}],
            None,
        ),
    },
    {
        "id": "Split strings on slashes to make string arrays 1",
        "specs": (
            {"a": "1/2/3"},
            {
                "type": "object",
                "properties": {
                    "a": {"type": "array", "items": {"type": "string", "_splitOn": "/"}}
                },
            },
            {"a": ["1", "2", "3"]},
            None,
        ),
    },
    {
        "id": "Split strings on slashes to make string arrays 2",
        "specs": (
            {"a": ["1/2/3"]},
            {
                "type": "object",
                "properties": {
                    "a": {"type": "array", "items": {"type": "string", "_splitOn": "/"}}
                },
            },
            {"a": ["1", "2", "3"]},
            None,
        ),
    },
    {
        "id": "Split strings on slashes to make string arrays 3",
        "specs": (
            {"a": ["foo", "1/2/3", "bar"]},
            {
                "type": "object",
                "properties": {
                    "a": {"type": "array", "items": {"type": "string", "_splitOn": "/"}}
                },
            },
            {"a": ["foo", "1", "2", "3", "bar"]},
            None,
        ),
    },
    {
        "id": "Split strings on slashes to make integer arrays",
        "specs": (
            {"a": "1/2"},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "array",
                        "items": {"type": "integer", "_splitOn": "/"},
                    }
                },
            },
            {"a": [1, 2]},
            None,
        ),
    },
    {
        "id": "Deletion of short strings 1",
        "specs": (
            {"a": "ab", "b": ["uvw", "xyz"]},
            {
                "type": "object",
                "properties": {
                    "a": {"type": "string", "minLength": 2, "_deleteIfShort": True},
                    "b": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "minLength": 3,
                            "_deleteIfShort": True,
                        },
                    },
                },
            },
            {"a": "ab", "b": ["uvw", "xyz"]},
            None,
        ),
    },
    {
        "id": "Deletion of short strings 2",
        "specs": (
            {"a": "b", "b": ["uvw", "xy"]},
            {
                "type": "object",
                "properties": {
                    "a": {"type": "string", "minLength": 2, "_deleteIfShort": True},
                    "b": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "minLength": 3,
                            "_deleteIfShort": True,
                        },
                    },
                },
            },
            {"b": ["uvw"]},
            None,
        ),
    },
    {
        "id": "Error message on incorrect-length strings 1",
        "specs": (
            {"a": "1"},
            {"type": "object", "properties": {"a": {"type": "string", "minLength": 2}}},
            pytest.raises(InvalidRequest),
            (
                "request['a']: '1': string is too short. Should have at least 2 character(s)",
            ),
        ),
    },
    {
        "id": "Error message on incorrect-length strings 2",
        "specs": (
            {"a": "1"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "minLength": 2, "maxLength": 2}},
            },
            pytest.raises(InvalidRequest),
            (
                "request['a']: '1': string is too short. Should have exactly 2 character(s)",
            ),
        ),
    },
    {
        "id": "Error message on incorrect-length strings 3",
        "specs": (
            {"a": "123"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "maxLength": 2}},
            },
            pytest.raises(InvalidRequest),
            (
                "request['a']: '123': string is too long. Should have no more than 2 character(s)",
            ),
        ),
    },
    {
        "id": "Strings not matching a regex 1",
        "specs": (
            {"a": "1"},
            {
                "type": "object",
                "patternProperties": {".*": {"type": "string", "pattern": "[^ ]+"}},
            },
            {"a": "1"},
            None,
        ),
    },
    {
        "id": "Strings not matching a regex 2",
        "specs": (
            {"a": "  "},
            {
                "type": "object",
                "patternProperties": {".*": {"type": "string", "pattern": "[^ ]+"}},
            },
            pytest.raises(InvalidRequest),
            (
                "request['a']: invalid value: '  '. Valid values must match regular expression '[^ ]+'",
            ),
        ),
    },
    {
        "id": "Check suppression of regex in error message",
        "specs": (
            {"a": "  "},
            {
                "type": "object",
                "patternProperties": {
                    ".*": {
                        "type": "string",
                        "pattern": "[^ ]+",
                        "_onErrorShowPattern": False,
                    }
                },
            },
            pytest.raises(InvalidRequest),
            ("request['a']: invalid value: '  '",),
        ),
    },
    {
        "id": "Remove duplicate list items",
        "specs": (
            {"foo": ["a", "b", "c", "a"]},
            {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "array",
                        "items": {"type": "string"},
                        "uniqueItems": True,
                    }
                },
            },
            {"foo": ["a", "b", "c"]},
            None,
        ),
    },
    {
        "id": "Remove duplicate list items in combination with string splitting",
        "specs": (
            {"foo": "a/b/c/a"},
            {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "array",
                        "items": {"type": "string", "_splitOn": "/"},
                        "uniqueItems": True,
                    }
                },
            },
            {"foo": ["a", "b", "c"]},
            None,
        ),
    },
    {
        "id": "Fail on duplicate list items",
        "specs": (
            {"foo": ["a", "b", "c", "a"]},
            {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "array",
                        "items": {"type": "string"},
                        "uniqueItems": True,
                        "_noRemoveDuplicates": True,
                    }
                },
            },
            pytest.raises(InvalidRequest),
            ("request['foo']: has repeated values in the list, e.g. 'a'",),
        ),
    },
    {
        "id": "Removal of invalid dict keys 1",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {"type": "object", "properties": {"foo": {"type": "string"}}},
            {"foo": "bar", "hash": "tag"},
            None,
        ),
    },
    {
        "id": "Removal of invalid dict keys 2",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "_removeAdditional": False,
            },
            {"foo": "bar", "hash": "tag"},
            None,
        ),
    },
    {
        "id": "Removal of invalid dict keys 3",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "_removeAdditional": True,
            },
            {"foo": "bar", "hash": "tag"},
            None,
        ),
    },
    {
        "id": "Removal of invalid dict keys 4",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": True,
            },
            {"foo": "bar", "hash": "tag"},
            None,
        ),
    },
    {
        "id": "Removal of invalid dict keys 5",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": True,
                "_removeAdditional": False,
            },
            {"foo": "bar", "hash": "tag"},
            None,
        ),
    },
    {
        "id": "Removal of invalid dict keys 6",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": True,
                "_removeAdditional": True,
            },
            {"foo": "bar", "hash": "tag"},
            None,
        ),
    },
    {
        "id": "Removal of invalid dict keys 7",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": False,
            },
            pytest.raises(InvalidRequest),
            ("request: Invalid key name: 'hash'",),
        ),
    },
    {
        "id": "Removal of invalid dict keys 8",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": False,
                "_removeAdditional": False,
            },
            pytest.raises(InvalidRequest),
            ("request: Invalid key name: 'hash'",),
        ),
    },
    {
        "id": "Removal of invalid dict keys 9",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": False,
                "_removeAdditional": True,
            },
            {"foo": "bar"},
            None,
        ),
    },
    {
        "id": "Removal of invalid dict keys 10",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": False,
                "_removeAdditional": [],
            },
            pytest.raises(InvalidRequest),
            ("request: Invalid key name: 'hash'",),
        ),
    },
    {
        "id": "Removal of invalid dict keys 11",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": False,
                "_removeAdditional": ["foo", "bar"],
            },
            pytest.raises(InvalidRequest),
            ("request: Invalid key name: 'hash'",),
        ),
    },
    {
        "id": "Removal of invalid dict keys 12",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": False,
                "_removeAdditional": ["hash"],
            },
            {"foo": "bar"},
            None,
        ),
    },
    {
        "id": "Removal of invalid dict keys 13",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": False,
                "_removeAdditional": ["foo", "hash"],
            },
            {"foo": "bar"},
            None,
        ),
    },
    {
        "id": "Error message for >1 invalid keys",
        "specs": (
            {"bar": 1, "hash": 2},
            {
                "type": "object",
                "properties": {"foo": {"type": "string"}},
                "additionalProperties": False,
            },
            pytest.raises(InvalidRequest),
            ("request: Invalid key names: 'bar', 'hash'",),
        ),
    },
    {
        "id": "Error message for >1 invalid keys when patternProperties is used",
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "patternProperties": {"foo.*": {"type": "string"}},
                "additionalProperties": False,
            },
            pytest.raises(InvalidRequest),
            (
                "request: 'hash' is an invalid key name because it does not match "
                "one of the following regular expressions: 'foo.*'",
            ),
        ),
    },
    {
        "id": (
            "Error message for >1 invalid keys when patternProperties is used "
            "and check suppression of regex in the error message"
        ),
        "specs": (
            {"foo": "bar", "hash": "tag"},
            {
                "type": "object",
                "patternProperties": {"foo.*": {"type": "string"}},
                "additionalProperties": False,
                "_onErrorShowPattern": False,
            },
            pytest.raises(InvalidRequest),
            ("request: 'hash' is an invalid key name",),
        ),
    },
    {
        "id": "Error message for invalid dict keys when propertyNames is used",
        "specs": (
            [{"x": 1}, {"0": 42}],
            {
                "type": "array",
                "items": {"type": "object", "propertyNames": {"pattern": "^[A-Za-z_]"}},
            },
            pytest.raises(InvalidRequest),
            (
                "request: '0' is an invalid key name because it "
                "does not match the following regular expression: '^[A-Za-z_]'",
            ),
        ),
    },
    {
        "id": "Error message for wrong number of keys 1",
        "specs": (
            {"a": 1},
            {"type": "object", "minProperties": 2, "maxProperties": 3},
            pytest.raises(InvalidRequest),
            ("request: {'a': 1} does not have enough keys",),
        ),
    },
    {
        "id": "Error message for wrong number of keys 2",
        "specs": (
            {"a": 1, "b": 2, "c": 3, "d": 4},
            {"type": "object", "minProperties": 2, "maxProperties": 3},
            pytest.raises(InvalidRequest),
            ("request: {'a': 1, 'b': 2, 'c': 3, 'd': 4} has too many keys",),
        ),
    },
    {
        "id": "Reformat invalid date",
        "specs": (
            {"foo": "01022003"},
            {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "string",
                        "format": "date",
                        "_allowedFormats": "%d%m%Y",
                    }
                },
            },
            {"foo": "2003-02-01"},
            None,
        ),
    },
    {
        "id": "Reformat invalid date and allow >1 format",
        "specs": (
            {"foo": "2004:05:06"},
            {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "string",
                        "format": "date",
                        "_allowedFormats": ["%d%m%Y", "%Y:%m:%d"],
                    }
                },
            },
            {"foo": "2004-05-06"},
            None,
        ),
    },
    {
        "id": "Reformat invalid date and check error message on bad format",
        "specs": (
            {"foo": "2004"},
            {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "string",
                        "format": "date",
                        "_allowedFormats": ["%d%m%Y", "%Y:%m:%d"],
                    }
                },
            },
            pytest.raises(InvalidRequest),
            (
                "request['foo']: \"2004\" is not a valid "
                "date. Expected format is yyyy-mm-dd",
            ),
        ),
    },
    {
        "id": "Reformat invalid date and error message when there are no allowed alternative formats",
        "specs": (
            {"foo": "2004:05:06"},
            {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "string",
                        "format": "date",
                    }
                },
            },
            pytest.raises(InvalidRequest),
            (
                "request['foo']: \"2004:05:06\" is not a "
                "valid date. Expected format is yyyy-mm-dd",
            ),
        ),
    },
    {
        "id": "Date conversion from integer",
        "specs": (
            {"foo": 20040506},
            {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "string",
                        "format": "date",
                        "_allowedFormats": "%Y%m%d",
                    }
                },
            },
            {"foo": "2004-05-06"},
            None,
        ),
    },
    {
        "id": "Date-range splitting 1",
        "specs": (
            {"date": "2003-04-05/2006-07-08"},
            {
                "type": "object",
                "properties": {
                    "date": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "format": "date",
                            "_allowedFormats": ["%Y%m%d"],
                            "_splitOn": "/",
                        },
                    }
                },
            },
            {"date": ["2003-04-05", "2006-07-08"]},
            None,
        ),
    },
    {
        "id": "Date-range splitting 2",
        "specs": (
            {"date": "20030405/20060708"},
            {
                "type": "object",
                "properties": {
                    "date": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "format": "date",
                            "_allowedFormats": ["%Y%m%d"],
                            "_splitOn": "/",
                        },
                    }
                },
            },
            {"date": ["2003-04-05", "2006-07-08"]},
            None,
        ),
    },
    {
        "id": "Set defaults for missing keys 1",
        "specs": (
            {},
            {
                "type": "object",
                "required": ["a"],
                "properties": {"a": {"type": "string"}},
                "_defaults": {"a": "foo"},
            },
            {"a": "foo"},
            None,
        ),
    },
    {
        "id": "Set defaults for missing keys 2",
        "specs": (
            {"a": "bar"},
            {
                "type": "object",
                "required": ["a"],
                "properties": {"a": {"type": "string"}},
                "_defaults": {"a": "foo"},
            },
            {"a": "bar"},
            None,
        ),
    },
    {
        "id": "Set defaults for missing keys 3",
        "specs": (
            {},
            {
                "type": "object",
                "required": ["a", "b"],
                "properties": {
                    "a": {"type": "array", "items": {"type": "string", "_splitOn": "/"}}
                },
                "_defaults": {"a": "foo/bar", "b": "bat"},
            },
            {"a": ["foo", "bar"], "b": "bat"},
            None,
        ),
    },
    {
        "id": "Set defaults for missing keys 4",
        "specs": (
            {"a": "1/2"},
            {
                "type": "object",
                "required": ["a", "b"],
                "properties": {
                    "a": {"type": "array", "items": {"type": "string", "_splitOn": "/"}}
                },
                "_defaults": {"a": "foo/bar", "b": "bat"},
            },
            {"a": ["1", "2"], "b": "bat"},
            None,
        ),
    },
    {
        "id": "Set defaults for missing keys 5",
        "specs": (
            {"a": [1, 2], "b": "hello"},
            {
                "type": "object",
                "required": ["a", "b"],
                "properties": {
                    "a": {"type": "array", "items": {"type": "string", "_splitOn": "/"}}
                },
                "_defaults": {"a": "foo/bar", "b": "bat"},
            },
            {"a": ["1", "2"], "b": "hello"},
            None,
        ),
    },
    {
        "id": "Check error message for missing required keys",
        "specs": (
            {"a": 1},
            {
                "type": "object",
                "properties": {"a": {"type": "string"}, "b": {"type": "string"}},
                "required": ["a", "b"],
            },
            pytest.raises(InvalidRequest),
            ("request: missing mandatory key: 'b'",),
        ),
    },
    {
        "id": "Check error message for format='numeric string' 1",
        "specs": (
            {"a": "123.4"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "numeric string"}},
            },
            {"a": "123.4"},
            None,
        ),
    },
    {
        "id": "Check error message for format='numeric string' 2",
        "specs": (
            {"a": "123.4x"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "numeric string"}},
            },
            pytest.raises(InvalidRequest),
            ("request['a']: '123.4x' is not a valid numeric string",),
        ),
    },
    {
        "id": "Check error message for format='positive numeric string' 1",
        "specs": (
            {"a": "123.4"},
            {
                "type": "object",
                "properties": {
                    "a": {"type": "string", "format": "positive numeric string"}
                },
            },
            {"a": "123.4"},
            None,
        ),
    },
    {
        "id": "Check error message for format='positive numeric string' 2",
        "specs": (
            {"a": "-123.4"},
            {
                "type": "object",
                "properties": {
                    "a": {"type": "string", "format": "positive numeric string"}
                },
            },
            pytest.raises(InvalidRequest),
            ("request['a']: '-123.4' is not a valid positive numeric string",),
        ),
    },
    {
        "id": "Date range formats (date range) 1",
        "specs": (
            {"a": "2003-04-05/2006-07-08"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date range"}},
            },
            {"a": "2003-04-05/2006-07-08"},
            None,
        ),
    },
    {
        "id": "Date range formats (date range) 2",
        "specs": (
            {"a": "2003-04-05"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date range"}},
            },
            {"a": "2003-04-05/2003-04-05"},
            None,
        ),
    },
    {
        "id": "Date range formats (date range) 3",
        "specs": (
            {"a": "20030405/20060708"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date range"}},
            },
            pytest.raises(InvalidRequest),
            ("request['a']: '20030405/20060708' is not a valid date range",),
        ),
    },
    {
        "id": "Date range formats (date range) 4",
        "specs": (
            {"a": "2003-04-05/2006-07-08/2009-10-11"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date range"}},
            },
            pytest.raises(InvalidRequest),
            (
                "request['a']: '2003-04-05/2006-07-08/2009-10-11' is not a valid date range",
            ),
        ),
    },
    {
        "id": "Date range formats (date range) 5",
        "specs": (
            {"a": "2003-02-29/2003-03-01"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date range"}},
            },
            pytest.raises(InvalidRequest),
            ("request['a']: '2003-02-29/2003-03-01' is not a valid date range",),
        ),
    },
    {
        "id": "Date range formats (date range) 6",
        "specs": (
            {"a": "2003-01-02/2003-01-01"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date range"}},
            },
            pytest.raises(InvalidRequest),
            ("request['a']: '2003-01-02/2003-01-01' is not a valid date range",),
        ),
    },
    {
        "id": "Date range formats (date range) 7",
        "specs": (
            {"a": "20030405"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date range"}},
            },
            pytest.raises(InvalidRequest),
            ("request['a']: '20030405' is not a valid date range",),
        ),
    },
    {
        "id": "Date range formats (date range) 8",
        "specs": (
            {"a": "20030405"},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "string",
                        "format": "date range",
                        "_allowedFormats": "%Y%m%d",
                    }
                },
            },
            {"a": "2003-04-05/2003-04-05"},
            None,
        ),
    },
    {
        "id": "Date range formats (date range) 9",
        "specs": (
            {"a": 20030405},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "string",
                        "format": "date range",
                        "_allowedFormats": "%Y%m%d",
                    }
                },
            },
            {"a": "2003-04-05/2003-04-05"},
            None,
        ),
    },
    {
        "id": "Date range formats (date range) 10",
        "specs": (
            {"a": []},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "array",
                        "items": {"type": "string", "format": "date range"},
                    }
                },
            },
            {"a": []},
            None,
        ),
    },
    {
        "id": "Date range formats (date range) 11",
        "specs": (
            {"a": "2003-04-05"},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "array",
                        "items": {"type": "string", "format": "date range"},
                    }
                },
            },
            {"a": ["2003-04-05/2003-04-05"]},
            None,
        ),
    },
    {
        "id": "Date range formats (date range) 12",
        "specs": (
            {"a": ["2003-04-05", "2003-04-06"]},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "array",
                        "items": {"type": "string", "format": "date range"},
                    }
                },
            },
            {"a": ["2003-04-05/2003-04-05", "2003-04-06/2003-04-06"]},
            None,
        ),
    },
    {
        "id": "Date range formats (date or date range) 1",
        "specs": (
            {"a": "2003-04-05/2006-07-08"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date or date range"}},
            },
            {"a": "2003-04-05/2006-07-08"},
            None,
        ),
    },
    {
        "id": "Date range formats (date or date range) 2",
        "specs": (
            {"a": "2003-04-05"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date or date range"}},
            },
            {"a": "2003-04-05"},
            None,
        ),
    },
    {
        "id": "Date range formats (date or date range) 3",
        "specs": (
            {"a": "20030405/20060708"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date or date range"}},
            },
            pytest.raises(InvalidRequest),
            ("request['a']: '20030405/20060708' is not a valid date or date range",),
        ),
    },
    {
        "id": "Date range formats (date or date range) 4",
        "specs": (
            {"a": "2003-04-05/2006-07-08/2009-10-11"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date or date range"}},
            },
            pytest.raises(InvalidRequest),
            (
                "request['a']: '2003-04-05/2006-07-08/2009-10-11' is not a valid date or date range",
            ),
        ),
    },
    {
        "id": "Date range formats (date or date range) 5",
        "specs": (
            {"a": "2003-02-29/2003-03-01"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date or date range"}},
            },
            pytest.raises(InvalidRequest),
            (
                "request['a']: '2003-02-29/2003-03-01' is not a valid date or date range",
            ),
        ),
    },
    {
        "id": "Date range formats (date or date range) 6",
        "specs": (
            {"a": "2003-01-02/2003-01-01"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date or date range"}},
            },
            pytest.raises(InvalidRequest),
            (
                "request['a']: '2003-01-02/2003-01-01' is not a valid date or date range",
            ),
        ),
    },
    {
        "id": "Date range formats (date or date range) 7",
        "specs": (
            {"a": "20030405"},
            {
                "type": "object",
                "properties": {"a": {"type": "string", "format": "date or date range"}},
            },
            pytest.raises(InvalidRequest),
            ("request['a']: '20030405' is not a valid date or date range",),
        ),
    },
    {
        "id": "Date range formats (date or date range) 8",
        "specs": (
            {"a": "20030405"},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "string",
                        "format": "date or date range",
                        "_allowedFormats": "%Y%m%d",
                    }
                },
            },
            {"a": "2003-04-05"},
            None,
        ),
    },
    {
        "id": "Date range formats (date or date range) 9",
        "specs": (
            {"a": 20030405},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "string",
                        "format": "date or date range",
                        "_allowedFormats": "%Y%m%d",
                    }
                },
            },
            {"a": "2003-04-05"},
            None,
        ),
    },
    {
        "id": "Date range formats (date or date range) 10",
        "specs": (
            {"a": []},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "array",
                        "items": {"type": "string", "format": "date or date range"},
                    }
                },
            },
            {"a": []},
            None,
        ),
    },
    {
        "id": "Date range formats (date or date range) 11",
        "specs": (
            {"a": "2003-04-05"},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "array",
                        "items": {"type": "string", "format": "date or date range"},
                    }
                },
            },
            {"a": ["2003-04-05"]},
            None,
        ),
    },
    {
        "id": "Date range formats (date or date range) 12",
        "specs": (
            {"a": ["2003-04-05", "2003-04-06"]},
            {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "array",
                        "items": {"type": "string", "format": "date or date range"},
                    }
                },
            },
            {"a": ["2003-04-05", "2003-04-06"]},
            None,
        ),
    },
    # Test MARS-style schema. Note that care must be taken in the
    # patternProperties regex to exclude properties key names which might
    # provide a conflicting definition.
    {
        "id": "MARS-style schema 1",
        "specs": (
            {},
            MARS_STYLE_SCHEMA,
            [{}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 2",
        "specs": (
            [{}],
            MARS_STYLE_SCHEMA,
            [{}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 3",
        "specs": (
            [{"a": "1"}],
            MARS_STYLE_SCHEMA,
            [{"a": ["1"]}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 4",
        "specs": (
            [{"a": ["1"]}],
            MARS_STYLE_SCHEMA,
            [{"a": ["1"]}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 5",
        "specs": (
            [{"a": 1}],
            MARS_STYLE_SCHEMA,
            [{"a": ["1"]}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 6",
        "specs": (
            [{"a": "1/2"}],
            MARS_STYLE_SCHEMA,
            [{"a": ["1/2"]}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 7",
        "specs": (
            [{"a": "1/2", "area": "3/4/5/6"}],
            MARS_STYLE_SCHEMA,
            [{"a": ["1/2"], "area": [3.0, 4.0, 5.0, 6.0]}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 8",
        "specs": (
            {"area": ["3", "4", "5", "6"]},
            MARS_STYLE_SCHEMA,
            [{"area": [3.0, 4.0, 5.0, 6.0]}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 9",
        "specs": (
            {"area": [3, 4, 5, 6]},
            MARS_STYLE_SCHEMA,
            [{"area": [3.0, 4.0, 5.0, 6.0]}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 10",
        "specs": (
            {"area": "3/4/5/6/7"},
            MARS_STYLE_SCHEMA,
            pytest.raises(InvalidRequest),
            (
                "request['area']: [3.0, 4.0, 5.0, 6.0, '7']: list has too many items. Should have exactly 4",
            ),
        ),
    },
    {
        "id": "MARS-style schema 11",
        "specs": (
            {"area": "3/4/5"},
            MARS_STYLE_SCHEMA,
            pytest.raises(InvalidRequest),
            (
                "request['area']: [3.0, 4.0, 5.0]: list has too few items. Should have exactly 4",
            ),
        ),
    },
    {
        "id": "MARS-style schema 12",
        "specs": (
            {"area": "3/4/500/6"},
            MARS_STYLE_SCHEMA,
            pytest.raises(InvalidRequest),
            ("request['area'][2]: 500.0 is greater than the maximum of 90.0",),
        ),
    },
    {
        "id": "MARS-style schema 13",
        "specs": (
            {"area": "-300/4/50/6"},
            MARS_STYLE_SCHEMA,
            pytest.raises(InvalidRequest),
            ("request['area'][0]: -300.0 is less than the minimum of -90.0",),
        ),
    },
    {
        "id": "MARS-style schema 14",
        "specs": (
            {"area": "-3/4/5/6", "": "x"},
            MARS_STYLE_SCHEMA,
            pytest.raises(InvalidRequest),
            (
                "request: '' is an invalid key name because it does not "
                "match one of the following regular expressions: '^(?!area$).+$'",
            ),
        ),
    },
    {
        "id": "MARS-style schema 15",
        "specs": (
            {"date": "2003-04-05"},
            MARS_STYLE_SCHEMA,
            [{"date": ["2003-04-05"]}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 16",
        "specs": (
            {"date": ["2003-04-05", "2006-07-08"]},
            MARS_STYLE_SCHEMA,
            [{"date": ["2003-04-05", "2006-07-08"]}],
            None,
        ),
    },
    {
        "id": "MARS-style schema 17",
        "specs": (
            {"date": ["20030405", "20060708"]},
            MARS_STYLE_SCHEMA,
            [{"date": ["2003-04-05", "2006-07-08"]}],
            None,
        ),
    },
    {
        "id": "Actual MARS schema 1",
        "specs": (
            {},
            ACTUAL_MARS_SCHEMA,
            pytest.raises(InvalidRequest),
            ("request: {} should be non-empty",),
        ),
    },
    {
        "id": "Actual MARS schema 2",
        "specs": (
            {"a": []},
            ACTUAL_MARS_SCHEMA,
            pytest.raises(InvalidRequest),
            ("request['a']: []: list has too few items. Should have at least 1",),
        ),
    },
    {
        "id": "Actual MARS schema 3",
        "specs": (
            {"a": ["1", ""]},
            ACTUAL_MARS_SCHEMA,
            pytest.raises(InvalidRequest),
            (
                "request['a'][1]: '': string is too short. Should have at least 1 character(s)",
            ),
        ),
    },
    {
        "id": "Actual MARS schema 4",
        "specs": (
            {" a": ["1"]},
            ACTUAL_MARS_SCHEMA,
            pytest.raises(InvalidRequest),
            (
                "request: ' a' is an invalid key name because it does not "
                "match one of the following regular expressions: '^[^ ]+$'",
            ),
        ),
    },
    {
        "id": "Actual MARS schema 5",
        "specs": (
            {"a": 1},
            ACTUAL_MARS_SCHEMA,
            [{"a": ["1"]}],
            None,
        ),
    },
]


def generate_schema_drafts_specs():
    # We always test with draft-7
    drafts = ["7"]

    # Test with more recent drafts if the jsonschema version supports them
    if packaging.version.parse(jsonschema.__version__) > packaging.version.parse(
        "3.2.0"
    ):
        drafts.extend(["201909", "202012"])

    drafts_specs = []
    for draft in drafts:
        with open(
            os.path.join(
                os.path.dirname(jsonschema_specifications.__file__),
                "schemas",
                f"draft{draft}",
                "metaschema.json",
            )
        ) as f:
            draft_uri = json.load(f)["$schema"]
        draft_specs = {
            "draft": draft,
            "uri": draft_uri,
            "post_201909": (int(re.sub(r"[^\d]", "", draft)) > 201909),
        }
        drafts_specs.append(draft_specs)

    return drafts_specs


@pytest.mark.parametrize(
    "req,schema,expected,err_msgs",
    [test_case["specs"] for test_case in TEST_CASES],
    ids=[test_case["id"] for test_case in TEST_CASES],
)
@pytest.mark.parametrize(
    "draft_specs",
    generate_schema_drafts_specs(),
    ids=[
        f"draft-{draft_specs['draft']}"
        for draft_specs in generate_schema_drafts_specs()
    ],
)
def test_enforce(draft_specs, req, schema, expected, err_msgs):
    schema = schema.copy()
    schema["_draft"] = draft_specs["draft"]
    # Input schema are expected to follow the rules of draft 2019-09. If the
    # draft to test is later than that then we need to modify the input
    # schema to conform to the newer rules.
    if draft_specs["post_201909"]:
        schema = modernise_schema(schema)

    if isinstance(expected, pytest.RaisesExc):
        with expected as exc_info:
            enforce.enforce(req, schema)
        assert exc_info.value.args == err_msgs
    else:
        assert enforce.enforce(req, schema) == expected


def modernise_schema(schema):
    """
    For a schema written according to 2019-09 rules, output the equivalent
    according to 2020-12 rules.
    """
    schema_orig = schema
    schema = copy.deepcopy(schema)

    def to_202012(item, **kwargs):
        if isinstance(item, dict):
            # In 202012, "prefixItems" was introduced and the meaning of "items"
            # and "addtionalItems" changed
            if isinstance(item.get("items"), list):
                dict_key_rename(item, "items", "prefixItems")
            if "additionalItems" in item:
                dict_key_rename(item, "additionalItems", "items")

    enforce.recursive_call(schema, to_202012)

    # Print a comparison of the input and output schemas if they differ
    if schema != schema_orig:
        a = json.dumps(schema_orig, indent=2).split("\n")
        b = json.dumps(schema, indent=2).split("\n")
        a.extend([""] * max(len(b) - len(a), 0))
        b.extend([""] * max(len(a) - len(b), 0))
        lwidth = max([len(x) for x in a]) + 1
        txt = "Modernised schema:"
        for x, y in zip(a, b):
            txt += "\n" + x.ljust(lwidth) + ("*" if x != y else " ") + y
        # logger.debug(txt)

    return schema


def dict_key_rename(dict, old, new):
    """Rename a key without changing dict order."""
    for k, v in [(k, dict.pop(k)) for k in list(dict.keys())]:
        if k == old:
            k = new
        dict[k] = v
