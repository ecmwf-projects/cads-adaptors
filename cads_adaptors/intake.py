
import intake
import yaml


class Intake:
    """
    Class for handling intake format manifests for driving dataset entries.
    """

    @classmethod
    def from_yaml(cls, yaml_file="intake.yaml", **kwargs):
        with open(yaml_file, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
        cls(**config, **kwargs)

    def __init__(
            self,
            uri,
            source,
            constraints,
            groupby=None,
            facets=None,
            groups=None,
            list_file="intake-list.txt",
            tree_file="intake-tree.txt"
    ):
        self.uri = uri
        self.source = source
        self.constraints = constraints
        
        self._groupby = groupby
        self._facets = facets
        self._groups = groups
        
        self._list_file = list_file
        self._tree_file = tree_file

    @property
    def catalog(self):
        """Open the intake catalog and return an iterator over rows."""
        catalog = intake.open_catalog(self.uri)

        try:
            df = catalog[self.source].read()
        except KeyError:
            raise ValueError(f'No source "{self.source}" found in {self.uri}')

        if self._groupby:
            key = list(self._groupby.keys())[0]
            agg = self._groupby[key]
            df = df._groupby(key, as_index=False).aggregate(agg)

        for _, row in df.iterrows():
            yield row

    @property
    def facets(self):
        return [
            {self._map_facet(facet): row[facet] for facet in self._facets}
            for row in self.catalog
        ]

    def process(self, list_file='intake_list.txt', facets_file='facets.json'):
        """
        Using the configuration in intake.yaml, iterate over the catalog and
        write to intake_list.txt.
        """
        with open(list_file, 'w') as f:
            for row in self.catalog:
                constraints = []
                for constraint, keys in self.constraints.items():
                    keys = keys if isinstance(keys, list) else [keys]
                    values = [self.group_constraint(row, key) for key in keys]
                    constraints.append(f'{constraint}={"/".join(values)}')
                f.write(','.join(constraints) + '\n')

    def _map_facet(self, facet):
        """Map facet names to constraint names."""
        facets_to_constraints = {
            v: k for k, v in self.constraints.items() if isinstance(v, str)
        }
        return facets_to_constraints.get(facet, facet)

    def group_constraint(self, row, key):
        """
        Check if the value of a constraint should be grouped with other
        values.
        """
        value = row[key]

        groups = self.groups.get(key)
        if groups is not None:
            for name, group in groups.items():
                if value in group:
                    value = name