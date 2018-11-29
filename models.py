from assemblyline.datastore import odm


@odm.model(index=True, store=True)
class FileRow(odm.Model):
    parent = odm.Keyword(default_set=None)
    sha256 = odm.Keyword()


@odm.model(index=True, store=True)
class Submission(odm.Model):

    files = odm.List(odm.Compound(FileRow))
    metadata = odm.Mapping(odm.Text())

    selected_services = odm.List(odm.Keyword())
