import os
from assemblyline_core.dispatching.run_submissions import SubmissionDispatchServer
from assemblyline_core.dispatching.run_files import FileDispatchServer

# Pull in any environment customizations on how this will run
SUBMISSION_THREADS = int(os.environ.get('SUBMISSION_DISPATCH_THREADS', '1'))
FILE_THREADS = int(os.environ.get('FILE_DISPATCH_THREADS', '2'))

instances = []

try:
    # Start the right number of server threads
    for _ in range(SUBMISSION_THREADS):
        instances.append(SubmissionDispatchServer())
    for _ in range(FILE_THREADS):
        instances.append(FileDispatchServer())
    [_i.start() for _i in instances]

    # Wait for these threads to finish
    [_i.join() for _i in instances]

finally:
    # If for whatever reason we are kicked out of waiting, ask
    # that all the threads are exiting with you.
    for _i in instances:
        if _i.running:
            _i.stop()
    [_i.join() for _i in instances]
