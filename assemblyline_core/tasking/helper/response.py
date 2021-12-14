from sys import exc_info
from traceback import format_tb

from flask import jsonify, make_response, Response

from assemblyline.common.str_utils import safe_str
from assemblyline_core.tasking.config import VERSION, LOGGER
from assemblyline_core.tasking.helper.logger import log_with_traceback


def make_api_response(data, err="", status_code=200, cookies=None):
    if isinstance(err, Exception):
        trace = exc_info()[2]
        err = ''.join(['\n'] + format_tb(trace) + [f"{err.__class__.__name__}: {str(err)}\n"]).rstrip('\n')
        log_with_traceback(LOGGER, trace, "Exception", is_exception=True)

    resp = make_response(jsonify({"api_response": data,
                                  "api_error_message": err,
                                  "api_server_version": VERSION,
                                  "api_status_code": status_code}),
                         status_code)

    if isinstance(cookies, dict):
        for k, v in cookies.items():
            resp.set_cookie(k, v)

    return resp


def make_file_response(data, name, size, status_code=200, content_type="application/octet-stream"):
    response = make_response(data, status_code)
    response.headers["Content-Type"] = content_type
    response.headers["Content-Length"] = size
    response.headers["Content-Disposition"] = f'attachment; filename="{safe_str(name)}"'
    return response


def stream_file_response(reader, name, size, status_code=200):
    chunk_size = 65535

    def generate():
        reader.seek(0)
        while True:
            data = reader.read(chunk_size)
            if not data:
                break
            yield data

    headers = {"Content-Type": 'application/octet-stream',
               "Content-Length": size,
               "Content-Disposition": f'attachment; filename="{safe_str(name)}"'}
    return Response(generate(), status=status_code, headers=headers)


def make_binary_response(data, size, status_code=200):
    response = make_response(data, status_code)
    response.headers["Content-Type"] = 'application/octet-stream'
    response.headers["Content-Length"] = size
    return response


def stream_binary_response(reader, status_code=200):
    chunk_size = 4096

    def generate():
        reader.seek(0)
        while True:
            data = reader.read(chunk_size)
            if not data:
                break
            yield data

    return Response(generate(), status=status_code, mimetype='application/octet-stream')
