import rclpy
from rclpy.node import Node
import yaml
import sys
import os
import glob
import importlib
from dataclasses import dataclass
from typing import Any, Dict
from datetime import datetime

@dataclass
class TopicMonitorConfig:
	topic_name: str
	topic_type: str
	field: str
	timeout_ms: float
	timeout_response: str
	ranges: Dict[str, Dict[str, float]]

class HealthMonitor(Node):

	def __init__(self, config_dir):
		super().__init__('health_monitor')

		self.get_logger().info(f'Loading YAML configurations from: {config_dir}')

		self.topic_monitors = []
		for cfg in self.load_yaml_files(config_dir):
			self.topic_monitors.append(self.create_monitor(cfg))

		self.timer = self.create_timer(0.5, self.check_all_timeouts)

	def load_yaml_files(self, path):
		# TODO: Add basic validation for ranges of basic/error/etc.
		# TODO: Add potential for multiple non-continuious ranges
		# TODO: Look into support for non-numeric values (bool/enum)
		files = glob.glob(os.path.join(path, '*.yaml'))
		configs = []
		for file in files:
			with open(file, 'r') as f:
				configs.append(yaml.safe_load(f))
		return configs

	def create_monitor(self, cfg):
		topic_cfg = cfg['topic']
		monitor = TopicMonitorConfig(
			topic_name = topic_cfg['topic_name'],
			topic_type = topic_cfg['topic_type'],
			field = topic_cfg['field'],
			timeout_ms = topic_cfg['timeout']['timeout_time'],
			timeout_response = topic_cfg['timeout']['timeout_response'],
			ranges = {
				'normal': topic_cfg['normal_range'],
				'warning': topic_cfg['warning_range'],
				'error': topic_cfg['error_range'],
				'fatal': topic_cfg['fatal_range']
			}
		)

		msg_module_name, msg_class_name = monitor.topic_type.rsplit('.', 1)
		msg_module = importlib.import_module(msg_module_name)
		msg_class = getattr(msg_module, msg_class_name)

		monitor.last_msg_time = self.get_clock().now()
		monitor.last_status = 'unknown'

		self.create_subscription(msg_class, monitor.topic_name, lambda msg, m=monitor: self.callback(msg, m), 10)

		self.get_logger().info(f'Monitoring {monitor.topic_name}.{monitor.field}')
		return monitor

	def callback(self, msg, monitor):
		monitor.last_msg_time = self.get_clock().now()

		val = msg
		for attr in monitor.field.split('.'):
			val = getattr(val, attr)

		status = self.evaluate_value(val, monitor.ranges)

		# TODO: Make this dict work for message levels
		# levelDict = {
		# 	'normal': self.getlogger().info,
		# 	'warning': self.get_logger().warn,
		# 	'error': self.get_logger().error,
		# 	'fatal': self.get_logger().fatal
		# }

		self.get_logger().info(f'{monitor.topic_name}.{monitor.field} = {val:.3f} -> {status}')

		# TODO: Make this determine if a set amount of time has elapsed
		# # To log if state changes or periodically (avoid flooding)
		# if status != monitor.last_status:
		# 	self.get_logger().info(f'{monitor.topic_name}.{monitor.field} = {val:.3f} -> {status}')
		# 	monitor.last_status = status

	def evaluate_value(self, val, ranges):
		for level, limits in ranges.items():
			if limits['min'] <= val < limits['max']:
				return level
		return 'unknown'

	def check_all_timeouts(self):
		now = self.get_clock().now()
		for monitor in self.topic_monitors:
			elapsed = (now - monitor.last_msg_time).nanoseconds / 1e6
			if elapsed > monitor.timeout_ms:
				self.get_logger().warn(f'Timeout on {monitor.topic_name}.{monitor.field}: no message for {elapsed:.0f}ms -> {monitor.timeout_response}')

def main(args=None):
	rclpy.init()

	if len(sys.argv) < 2:
		config_dir = os.path.dirname(os.path.realpath(__file__))
		print(f'No path specified. Using current script directory: {config_dir}')
	else:
		config_dir = sys.argv[1]

	node = HealthMonitor(config_dir)

	try:
		rclpy.spin(node)
	except KeyboardInterrupt:
		pass
	finally:
		node.destroy_node()
		rclpy.shutdown()

if __name__ == '__main__':
	main()