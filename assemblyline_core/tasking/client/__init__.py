from os import environ

from flask import Flask

from assemblyline_core.tasking.client.api import file, safelist, service, task

SERVICE_API_HOST = environ.get('SERVICE_API_HOST', "http://service-server:5003")
SHA256_REGEX = r'[a-f0-9]{64}'
# How API calls to service-server map to the functions being called
# Provide a means of extracting parameters from API path
PATH_MAPPING = {
    rf'GET {SERVICE_API_HOST}/api/v1/file/{SHA256_REGEX}/': (file.download_file, {'sha256': SHA256_REGEX}),
    rf'PUT {SERVICE_API_HOST}/api/v1/file/': (file.upload_files, {}),
    rf'GET {SERVICE_API_HOST}/api/v1/safelist/{SHA256_REGEX}/': (safelist.exists, {'qhash': SHA256_REGEX}),
    rf'GET {SERVICE_API_HOST}/api/v1/safelist/': (safelist.get_safelist_for_tags, {}),
    rf'GET {SERVICE_API_HOST}/api/v1/safelist/signatures/': (safelist.get_safelist_for_signatures, {}),
    rf'PUT {SERVICE_API_HOST}/api/v1/service/register/': (service.register_service, {}),
    rf'GET {SERVICE_API_HOST}/api/v1/task/': (task.get_task, {}),
    rf'POST {SERVICE_API_HOST}/api/v1/task/': (task.task_finished, {}),
}
