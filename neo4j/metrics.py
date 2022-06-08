from dataclasses import dataclass


@dataclass
class ConnectionPoolMetrics:
    acquired: int = 0
    acquiring: int = 0
    closed: int = 0
    created: int = 0
    creating: int = 0
    failed_to_create: int = 0
    idle: int = 0
    in_use: int = 0
    timed_out_to_acquire: int = 0
    total_acquisition_time: int = 0
    total_connection_time: int = 0
    total_in_use_count: int = 0
    total_in_use_time: int = 0
