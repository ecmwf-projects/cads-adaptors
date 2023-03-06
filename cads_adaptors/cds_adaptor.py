from . import adaptor, constraints, costing


class CDSAdaptor(adaptor.AbstractAdaptor):
    def __init__(self, config, constraints, mapping, form, licences):
        self.config = config
        self.constraints = constraints
        self.form = form
        self.mapping = mapping
        self.licences = licences

    def validate(self, request):
        return True

    def apply_constraints(self, request):
        return constraints.apply_constraints(self.form, request, self.constraints)

    def estimate_costs(self, request):
        costs = {"size": costing.estimate_size(self.form, request, self.constraints)}
        return costs

    def get_licences(self, request):
        return self.licences
