import calendar
import typing as T

DEFAULT_DATE_FORMAT = "%Y-%m-%d"


def rooki_args(*keys):
    def decorator(method):
        def wrapper(self, *args, **kwargs):
            values = method(self, *args, **kwargs)
            return {keys[i]: values[i] for i in range(len(values))}

        return wrapper

    return decorator


def parse_date_string(date_string, date_format=DEFAULT_DATE_FORMAT):
    """Format any datestring into the required WPS date format."""
    import dateutil

    datetime = dateutil.parser.parse(date_string)
    return datetime.strftime(date_format)


class Operator:
    ROOKI: T.Union[str, None] = None

    def __init__(self, request):
        self.request = request

    @property
    def parameters(self):
        return (
            getattr(self, parameter)
            for parameter in dir(self)
            if not parameter.startswith("_")
        )

    @staticmethod
    def update_kwargs(target, source):
        """Combine rooki parameters to be passed as a single operator argument."""
        for key, value in source.items():
            if key in target:
                target[key] = "|".join((target[key], value)).rstrip("|")
            else:
                target[key] = value
        return target


class Subset(Operator):
    ROOKI: T.Union[str, None] = "Subset"

    def date(request):
        """
        Extract a date range from a CDS request and translate it to a rooki-style
        date range.
        """
        date_range = request["date"]

        if isinstance(date_range, list):
            start, end = str(date_range[0]), str(date_range[-1])
            date_range = f"{parse_date_string(start)}/{parse_date_string(end)}"

        return {"time": date_range}

    def year_range(self):
        """Convert a CDS-style year range to a rooki-style year range."""
        years = self.request["year"]
        if not isinstance(years, (list, tuple)):
            years = [years]
        return {"time": "/".join((min(years), max(years)))}

    def year(self):
        """Convert a CDS-style year request to a rooki-style year request."""
        years = self.request["year"]
        if not isinstance(years, (list, tuple)):
            years = [years]
        return {
            **self.year_range,
            **{"time_components": "year:" + ",".join(years)},
        }

    def month(self):
        """Convert a CDS-style month request to a rooki-style month request."""
        months = self.request["month"]
        if not isinstance(months, (list, tuple)):
            months = [months]
        months = [calendar.month_name[int(month)].lower()[:3] for month in months]
        return {"time_components": "month:" + ",".join(months)}

    def day(self):
        """Convert a CDS-style day request to a rooki-style day request."""
        days = self.request["day"]
        if not isinstance(days, (list, tuple)):
            days = [days]
        return {"time_components": "day:" + ",".join(days)}

    def level(self):
        """
        Extract a level argument from a CDS request and translate it to a
        rooki-style level range.
        """
        import re

        levels = self.request["level"]

        if not isinstance(levels, (list, tuple)):
            levels = [levels]

        levels = [
            list(re.finditer(r"[\d]*[.][\d]+|[\d]+", level))[0] for level in levels
        ]

        for i, level in enumerate(levels):
            try:
                levels[i] = int(float(level))
            except ValueError:
                raise KeyError

        levels = ",".join([str(level) for level in levels])

        return {"level": levels}

    def area(self):
        """
        Extract an area argument from a CDS request and translate it to a
        rooki-style geographical area range.
        """
        extents = self.request["area"]

        if isinstance(extents, str):
            delimiters = [",", "/"]
            for delimiter in delimiters:
                if delimiter in extents:
                    extents = extents.split(delimiter)
                    break
            else:
                raise ValueError(f"invalid area argument: {extents}")

        # reorder extents from MARS-style (NWSE) to rooki-style (WSEN)
        extents_order = [1, 2, 3, 0]
        extents = [extents[i] for i in extents_order]

        extents = ",".join(str(extent) for extent in extents)

        return {"area": extents}


ROOKI_OPERATORS = [
    Subset,
]
