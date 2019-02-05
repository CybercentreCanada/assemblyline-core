# import hashlib
#
# # TODO these need to be changed for renamed fields
# service_overrides = [
#     'deep_scan',
#     'eligible_parents',
#     'ignore_filtering',
#     'ignore_size',
#     'max_extracted',
#     'max_supplementary',
# ]
#
# submission_overrides = service_overrides + [
#     'classification',
#     'ignore_cache',
#     'params',
# ]
#
# # Keys that are hashed into the
# ingestion_overrides = submission_overrides + [
#     'completed_queue',
#     'description',
#     'generate_alert',
#     'groups',
#     'notification_queue',
#     'notification_threshold',
#     'psid',
#     'resubmit_to',
#     'scan_key',
#     'selected',
#     'submitter',
# ]
#
#
# def get_submission_overrides(getter, field_list=submission_overrides):
#     d = {}
#     for k in field_list:
#         v = getter.get(k)
#         if v is not None:
#             d[k] = v
#     return d
#
#
# def create_filescore_key(sha256, getter, selected=None):
#     # One up this if the cache is ever messed up and we
#     # need to quickly invalidate all old cache entries.
#     version = 0
#
#     d = get_submission_overrides(getter)
#     d['sha256'] = sha256
#     if selected:
#         d['selected'] = [str(x) for x in selected]
#
#     s = ', '.join([f"{k}: {d[k]}" for k in sorted(d.keys())])
#
#     return 'v'.join([str(hashlib.md5(s.encode()).hexdigest()), str(version)])
