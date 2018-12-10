from assemblyline.datastore import odm


def normalize_data(data):
    if isinstance(data, dict):
        return tuple((k, normalize_data(data[k])) for k in sorted(data.keys()))
    elif isinstance(data, (list, tuple)):
        return tuple(normalize_data(v) for v in data)
    else:
        return data


def config_hash(config):
    return hash(normalize_data(config))


def build_result_key(file_hash, service, config):
    return f"{file_hash}.{service}.{config_hash(config)}"


@odm.model(index=True, store=True)
class Submission(odm.Model):

    files = odm.List(odm.Keyword())
    metadata = odm.Mapping(odm.Text(), default={})

    selected_services = odm.List(odm.Keyword(), default=[])


@odm.model(index=True, store=True)
class Result(odm.Model):
    pass
