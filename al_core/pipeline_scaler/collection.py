"""
Code for collecting metric data and feeding it into the orchestration framework.

The task of this module is to take data available from heartbeats, and convert them
into a uniform format that can be consumed by the scaling system.
"""



# import math
# import logging
#
# from pipeline_scaler.smoothing import FlowData
# log = logging.getLogger(__name__)
#
#
# class FlowDirector:
#     def __init__(self, data_source: FlowData, component, grow_limit=100, shrink_limit=None, friction=0.1):
#         self.data = data_source
#         self.component = component
#         self.desired = self.data.instances
#
#         self.flux = 0
#         self.grow_limit = abs(grow_limit)
#         self.shrink_limit = -abs(shrink_limit) if shrink_limit is not None else -self.grow_limit
#         self.friction = friction
#
#     @property
#     def input_rate(self):
#         return self.data.backlog_rate + self.data.throughput
#
#     @property
#     def max_expected_throughput(self):
#         return self.data.per_unit_throughput / self.data.duty_cycle * self.desired
#
#     def growth_pressure(self):
#         # Should we scale up because of backlog
#         back_pressure = self.data.backlog/self.component.backlog
#         # Should we scale up/down because the input rate vs our expected rate
#         rate_pressure = (self.input_rate - self.max_expected_throughput)
#
#         pressure = back_pressure + rate_pressure
#         return math.copysign(math.sqrt(abs(pressure)), pressure)
#
#     def update(self, data):
#         # Include the new data in our averages
#         delta = data['delta']
#         self.data.update(data)
#
#         # Update the pressure to go up or down
#         self.flux += delta * self.growth_pressure()
#
#         # Apply the friction, tendency to do nothing, tendency of the 'what to do' bar
#         # to move to nothing over time when there is no strong up or down pressure
#         self.flux -= math.copysign(min(self.friction * delta, abs(self.flux)), self.flux)
#
#         if self.flux >= self.grow_limit:
#             if self.flux >= 2*self.grow_limit:
#                 log.warning("Unexpectedly fast growth pressure")
#             self.desired += 1
#             self.flux = 0
#
#         if self.flux <= self.shrink_limit:
#             if self.flux >= 2*self.shrink_limit:
#                 log.warning("Unexpectedly fast shrink pressure")
#             self.desired -= 1
#             self.flux = 0
#


#
# from typing import Union
#
#
# class WindowFlowData:
#     def __init__(self, window):
#         self.window = window
#         self._deltas = []
#         self._throughput = []
#         self._backlog_rate = []
#         self._per_unit_throughput = []
#         self._backlog = []
#         self._duty_cycle = []
#         self.instances = 0
#         self.backlog = 0
#         self.throughput = 0
#         self.per_unit_throughput = 0
#         self.backlog_rate = 0
#         self.duty_cycle = 0
#
#     @property
#     def ready(self):
#         return sum(self._deltas) >= self.window/2
#
#     def _read_buffer(self, buffer, offset=0):
#         time, total = 0, 0
#         for delta, value in zip(reversed(self._deltas), reversed(buffer)):
#             if offset > 0:
#                 offset -= delta
#
#                 if offset < 0:
#                     value *= -offset/delta
#                     delta = -offset
#                 else:
#                     continue
#
#             time += delta
#             total += value * delta
#
#             if time >= self.window:
#                 break
#
#         return total/time
#
#     def update(self, data):
#         delta = data['seconds']
#         self._deltas.append(delta)
#
#         self.instances = data['instances']
#         self._throughput.append(data['finished']/delta)
#         self._duty_cycle.append(data['busy_time']/delta)
#         self._backlog.append(data['backlog'])
#         self._per_unit_throughput.append(self._throughput[-1]/self.instances)
#
#         if len(self._backlog) > 2:
#             backlog_rate = (self._backlog[-1] - self._backlog[-2])/delta
#             self._backlog_rate.append(backlog_rate)
#
#         if self.ready:
#             self.per_unit_throughput = self._read_buffer(self._per_unit_throughput)
#             self.throughput = self._read_buffer(self._throughput)
#             self.backlog = self._read_buffer(self._backlog)
#             self.duty_cycle = self._read_buffer(self._duty_cycle)
#             if self._backlog_rate:
#                 self.backlog_rate = self._read_buffer(self._backlog_rate)
#
# #
# # class EMA:
# #     def __init__(self, alpha, rate_alpha=None):
# #         self.alpha = alpha
# #         self.rate_alpha = rate_alpha or (alpha + 1)/2
# #         self.value = None
# #         self.rate = None
# #
# #     def update(self, value):
# #         if self.value is None:
# #             self.value = value
# #         old = self.value
# #         self.value = value * self.alpha
# #
# #
# #
# # class EMAFlowData:
# #     def __init__(self, alpha):
# #         self.alpha = alpha
# #
# #         self.last_timestamp = 0
# #         self.step = 0
# #
# #         self.instances = 0
# #         self.throughput = 0
# #         self.per_unit_throughput = 0
# #         self.backlog = 0
# #
# #     def update(self, data):
# #         if self.step > 2:
# #             delta = data['time'] - self.last_timestamp
# #             self.instances = data['instances'] * self.alpha + self.instances * (1 - self.alpha)
# #             self.backlog = data['backlog'] * self.alpha + self.backlog * (1 - self.alpha)
# #
# #             throughput = data['finished']/delta
# #             self.throughput = throughput * self.alpha + self.throughput * (1 - self.alpha)
# #
# #             self.per_unit_throughput = throughput/data['instances'] * self.alpha + self.per_unit_throughput * (1 - self.alpha)
# #             self.last_timestamp = data['time']
# #         elif self.step == 0:
# #             self.instances = data['instances']
# #             self.backlog = data['backlog']
# #             self.last_timestamp = data['time']
# #         elif self.step == [1, 2]:
# #             delta = data['time'] - self.last_timestamp
# #             self.instances = data['instances'] * self.alpha + self.instances * (1 - self.alpha)
# #             self.backlog = data['backlog'] * self.alpha + self.backlog * (1 - self.alpha)
# #
# #             throughput = data['finished']/delta
# #             self.throughput = throughput
# #             self.per_unit_throughput = throughput/data['instances']
# #
# #             self.last_timestamp = data['time']
# #         self.step += 1
# #
#
# FlowData = Union[WindowFlowData]
