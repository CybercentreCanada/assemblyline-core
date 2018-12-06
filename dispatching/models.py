from assemblyline.datastore import odm


@odm.model(index=True, store=True)
class Submission(odm.Model):

    files = odm.List(odm.Keyword())
    metadata = odm.Mapping(odm.Text(), default={})

    selected_services = odm.List(odm.Keyword(), default=[])
