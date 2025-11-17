import rclpy
from rclpy.node import Node
from rclpy.time import Time
import yaml
import sys
import os
import glob
import importlib
from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class TopicMonitorConfig:
    topic_name: str
    topic_type: str
    field: str
    timeout_period: float
    timeout_status: str
    ranges: Dict[str, Dict[str, float]]
    last_msg_time: Time
    last_status: str
    last_value: Any


class HealthMonitor(Node):
    def __init__(self, config_dir):
        super().__init__("health_monitor", namespace="health_monitor")

        self.get_logger().info(f"Loading YAML configurations from: {config_dir}")

        self.topic_monitors: list[TopicMonitorConfig] = []
        for cfg in self.load_yaml_files(config_dir):
            self.topic_monitors.append(self.create_monitor(cfg))

        self.timeout_timer = self.create_timer(0.1, self.check_all_timeouts)

        self.status_publisher = self.create_publisher(DiagnosticArray, "status", 10)
        self.status_timer = self.create_timer(0.1, self.output_status)

    def load_yaml_files(self, path):
        # TODO: Add basic validation for ranges of basic/error/etc.
        # TODO: Add potential for multiple non-continuious ranges
        # TODO: Look into support for non-numeric values (bool/enum)
        files = glob.glob(os.path.join(path, "*.yaml"))
        configs = []
        for file in files:
            with open(file, "r") as f:
                configs.append(yaml.safe_load(f))
        return configs

    def create_monitor(self, cfg):
        topic_cfg = cfg["topic"]

        # Testing to see if there is a field defining this as a bool, unsure if this is the approach I want to take
        if topic_cfg.get("is_bool") is not None:
            print("Is bool!")
        else:
            print("Is not bool!")

        monitor = TopicMonitorConfig(
            topic_name=topic_cfg["topic_name"],
            topic_type=topic_cfg["topic_type"],
            field=topic_cfg["field"],
            timeout_period=topic_cfg["timeout"]["period"],
            timeout_status=topic_cfg["timeout"]["status"],
            ranges={
                "ok": topic_cfg["ok_range"],
                "warn": topic_cfg["warn_range"],
                "error": topic_cfg["error_range"],
            },
            last_msg_time=self.get_clock().now(),
            last_status="error",
            last_value=None,
        )

        msg_module_name, msg_class_name = monitor.topic_type.rsplit(".", 1)
        msg_module = importlib.import_module(msg_module_name)
        msg_class = getattr(msg_module, msg_class_name)

        self.create_subscription(
            msg_class,
            monitor.topic_name,
            lambda msg, m=monitor: self.monitor_callback(msg, m),
            10,
        )

        self.get_logger().info(f"Monitoring {monitor.topic_name}.{monitor.field}")
        return monitor

    def monitor_callback(self, msg, monitor):
        monitor.last_msg_time = self.get_clock().now()

        val = msg
        for attr in monitor.field.split("."):
            val = getattr(val, attr)

        status = self.evaluate_value(val, monitor.ranges)

        monitor.last_status = status
        monitor.last_value = val

        # self.get_logger().info(
        #     f"{monitor.topic_name}.{monitor.field} = {val:.3f} -> {status}"
        # )

    def evaluate_value(self, val, ranges):
        for level, limits in ranges.items():
            if limits["min"] <= val < limits["max"]:
                return level
        return "error"

    def check_all_timeouts(self):
        now = self.get_clock().now()
        for monitor in self.topic_monitors:
            elapsed = (
                now - monitor.last_msg_time
            ).nanoseconds / 1e9  # Nanoseconds to seconds
            if elapsed > monitor.timeout_period:
                monitor.last_status = monitor.timeout_status

    def output_status(self):
        array = DiagnosticArray()
        array.header.stamp = self.get_clock().now().to_msg()

        level_map = {
            "ok": DiagnosticStatus.OK,
            "warn": DiagnosticStatus.WARN,
            "error": DiagnosticStatus.ERROR,
            "stale": DiagnosticStatus.STALE,
        }

        for monitor in self.topic_monitors:
            status = DiagnosticStatus()
            status.level = level_map[monitor.last_status]
            status.name = f"{monitor.topic_name}.{monitor.field}"
            status.message = monitor.last_status

            key_value = KeyValue()
            key_value.key = "value"
            key_value.value = str(monitor.last_value)
            status.values.append(key_value)

            array.status.append(status)

        self.status_publisher.publish(array)


def main(args=None):
    rclpy.init()

    if len(sys.argv) < 2:
        config_dir = os.path.dirname(os.path.realpath(__file__))
        print(f"No path specified. Using current script directory: {config_dir}")
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


if __name__ == "__main__":
    main()
