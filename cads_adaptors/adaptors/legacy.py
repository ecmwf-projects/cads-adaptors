from cads_adaptors.adaptors import Request, cds


class LegacyCdsAdaptor(cds.AbstractCdsAdaptor):
    def multi_retrieve(self, request: Request) -> cds.T_MULTI_RETRIEVE:
        import cdsapi

        # parse input options
        collection_id = self.config.pop("collection_id", None)
        if not collection_id:
            raise ValueError("collection_id is required in request")

        # retrieve data
        client = cdsapi.Client(self.config["url"], self.config["key"], retry_max=1)
        result_path = client.retrieve(collection_id, request).download()
        return open(result_path, "rb")
