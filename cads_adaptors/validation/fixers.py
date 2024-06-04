"""Collection of classes for fixing different types of schema errors."""

import re
from copy import deepcopy
from datetime import datetime

SENTINEL = object()


class BaseFixer:
    """Abstract base class for schema error fixers."""

    all_fixers = []

    def __init_subclass__(cls):
        """Register all subclasses as fixers."""
        BaseFixer.all_fixers.append(cls)

    def __init__(self, request, error):
        self.request = request
        self.error = error
        self._instance_branch = None
        self._parent = SENTINEL
        self._index = SENTINEL

    def fix(self):
        """
        Attempt to fix the error. If a change is made (which may be
        in-place) return the changed request, otherwise return None.
        """
        if self.relevant():
            # print(repr(self) + ' is relevant')
            original = deepcopy(self.request)
            self.action()
            if self.request != original:
                return self.request
        return None

    def __getattr__(self, name):
        """Return an attribute of self.error as if it was in self."""
        try:
            return getattr(self.error, name)
        except AttributeError:
            raise AttributeError(name + " not defined") from None

    def replace_instance(self, value):
        """Set a new value for the problematic instance."""
        # If the instance has no parent then replace the request itself
        if self.parent is None:
            self.request = value
        else:
            self.parent[self.index] = value

    @property
    def parent(self):
        """Return a reference to the parent of the problematic instance."""
        if self._parent is SENTINEL:
            branch = self.instance_branch()
            if branch:
                self._parent = branch[-1][0]
                self._index = branch[-1][1]
            else:
                self._parent = None
                self._index = None
        return self._parent

    @property
    def index(self):
        """Return the index of the problematic instance in its parent."""
        self.parent
        return self._index

    def instance_branch(self):
        """
        Return a list of (instance, index) giving references to, and
        indices of, each instance leaf on the branch leading to the
        erroneous instance.
        """
        if self._instance_branch is None:
            parent = self.request
            self._instance_branch = []
            for index in self.error.absolute_path:
                self._instance_branch.append((parent, index))
                parent = parent[index]
        return self._instance_branch

    def reformat_date(self):
        """
        Intended to be called by subclass action() methods to attempt to
        fix invalid date strings. Returns fixed date or None if fix not
        possible.
        """
        # Any alternative formats we're willing to convert from?
        fmts = self.schema.get("_allowedFormats")
        if fmts:
            if not isinstance(fmts, list):
                fmts = [fmts]
            for fmt in fmts:
                try:
                    new = datetime.strptime(str(self.instance), fmt)
                except Exception:
                    pass
                else:
                    return new.strftime("%Y-%m-%d")
        return None


class List2Scalar(BaseFixer):
    """Turns one-element lists into scalar objects."""

    def relevant(self):
        # The instance should not be an array but is?
        return (
            self.validator == "type"
            and self.validator_value != "array"
            and isinstance(self.instance, (list, tuple))
        )

    def action(self):
        if not self.schema.get("_noScalarise") and len(self.instance) == 1:
            self.replace_instance(self.instance[0])


class Scalar2List(BaseFixer):
    """Turns scalar objects into one-element lists."""

    def relevant(self):
        # The instance should be an array but isn't?
        return self.validator == "type" and self.validator_value == "array"

    def action(self):
        if not self.schema.get("_noListify"):
            self.replace_instance([self.instance])


class SplitOnSeparator(BaseFixer):
    """
    Splits string on a separator to make lists. Requires parent object is
    a list.
    """

    def relevant(self):
        # This is an invalid string in a list and a list-separator has been
        # provided that will allow us to split it up?
        return (
            isinstance(self.instance, str)
            and isinstance(self.parent, list)
            and self.schema.get("_splitOn")
            and self.schema["_splitOn"] in self.instance
        )

    def action(self):
        self.parent[:] = (
            self.parent[0 : self.index]
            + self.instance.split(self.schema["_splitOn"])
            + self.parent[self.index + 1 :]
        )


class Convert2Num(BaseFixer):
    """Converts to ints or floats."""

    def relevant(self):
        # This isn't a number but it should be?
        return self.validator == "type" and self.validator_value in [
            "number",
            "integer",
        ]

    def action(self):
        if not self.schema.get("_noCast"):
            func = {"integer": int, "number": float}.get(self.validator_value)
            try:
                new = func(self.instance)
            except Exception:
                pass
            else:
                # Replace instance with this new value unless a conversion from
                # float to int has resulted in lost precision
                if not (
                    func is int
                    and isinstance(self.instance, float)
                    and new != self.instance
                ):
                    self.replace_instance(new)


class Convert2String(BaseFixer):
    """Converts to string format."""

    def relevant(self):
        # This isn't a string but it should be?
        return self.validator == "type" and self.validator_value == "string"

    def action(self):
        if self.instance.__class__.__name__ in self.schema.get(
            "_allowCastFrom", ["int", "float"]
        ):
            self.replace_instance(str(self.instance))


class RemoveShortStrings(BaseFixer):
    """Deletes too-short strings."""

    def relevant(self):
        return self.validator == "minLength"

    def action(self):
        if self.schema.get("_deleteIfShort"):
            if isinstance(self.parent, dict):
                del self.parent[self.index]
            elif isinstance(self.parent, list):
                self.parent[:] = (
                    self.parent[0 : self.index] + self.parent[self.index + 1 :]
                )


class Uniquify(BaseFixer):
    """Removes duplicate items from lists."""

    def relevant(self):
        # A list whose items should be unique but aren't?
        return self.validator == "uniqueItems" and self.validator_value

    def action(self):
        if not self.schema.get("_noRemoveDuplicates"):
            newlist = []
            # Preserve order when removing duplicates
            for v in self.instance:
                if v not in newlist:
                    newlist.append(v)
            self.replace_instance(newlist)


class RemoveAdditional(BaseFixer):
    """Removes invalid dictionary keys."""

    def relevant(self):
        # Unwanted dictionary keys (aka object properties)?
        return (
            self.validator == "additionalProperties"
            and not self.validator_value
            and self.schema.get("_removeAdditional")
        )

    def action(self):
        # The allowed keys can be specified as an explicit list or as regexes
        allowed = self.schema.get("properties", {}).keys()
        allowed_regexes = [
            re.compile(k) for k in self.schema.get("patternProperties", {}).keys()
        ]

        # _removeAdditional can be True to trigger removal of any offending
        # keys, or a list providing a subset that can be removed
        to_remove = self.schema.get("_removeAdditional")
        if not isinstance(to_remove, list):
            # Remove any key
            to_remove = list(self.instance.keys())

        for k in list(self.instance.keys()):
            # If key is not allowed and is in list of those permitted to be
            # removed, then remove it
            if (
                k not in allowed
                and not [r for r in allowed_regexes if r.search(k)]
                and k in to_remove
            ):
                del self.instance[k]


class ReformatDate(BaseFixer):
    """Fixes date formats."""

    def relevant(self):
        # Invalid date format?
        return (
            self.validator == "format"
            and self.validator_value in ["date", "date or date range"]
            and self.schema.get("_allowedFormats")
        )

    def action(self):
        d = self.reformat_date()
        if d:
            self.replace_instance(d)


class ReformatDateRange(BaseFixer):
    """Fixes date ranges."""

    def relevant(self):
        # Invalid date range?
        return self.validator == "format" and self.validator_value == "date range"

    def action(self):
        # The range could be invalid because it's a) just a single date or b) a
        # single invalid but fixable date or c) something else. Case a can be
        # fixed by duplicating it with a slash inbetween. Case b can be fixed
        # in the same way after the date is reformatted. We don't want to try
        # putting a slash in case c because if it's still invalid the error
        # message will quote the altered but still wrong string, which may be
        # confusing for the user. We do not currently fix the case when there
        # are two slash-separated invalid but fixable dates but could do in
        # future.
        string = str(self.instance)
        try:
            datetime.strptime(string, "%Y-%m-%d")
        except Exception:
            single_date = self.reformat_date()
        else:
            single_date = string
        if single_date:
            self.replace_instance(f"{single_date}/{single_date}")


class SetDefault(BaseFixer):
    """Sets default for missing keys."""

    def relevant(self):
        return (
            self.validator == "required"
            and isinstance(self.instance, dict)
            and "_defaults" in self.schema
        )

    def action(self):
        for key in self.validator_value:
            if key in self.schema["_defaults"]:
                self.instance.setdefault(key, self.schema["_defaults"][key])
