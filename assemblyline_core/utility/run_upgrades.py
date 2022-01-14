"""
Run any upgrade tasks.

Upgrade tasks should be written such that its safe to invoke this
script repeatedly.
"""

from assemblyline_core.utility.upgrades import add_rabbit


if __name__ == '__main__':
    # Move data from redis to rabbit
    add_rabbit.drain_queues()
