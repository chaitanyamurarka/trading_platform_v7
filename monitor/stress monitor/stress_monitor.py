#!/usr/bin/env python3
"""
Comprehensive Performance Monitor for Stress Testing
Monitors system resources, application metrics, and database performance
"""

import psutil
import time
import json
import requests
import redis
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
import threading
import csv
import os
from influxdb_client import InfluxDBClient
import asyncio
import aiohttp

class StressTestMonitor:
    def __init__(self, config: Dict):
        self.config = config
        self.monitoring = False
        self.metrics_data = []
        self.start_time = None
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'stress_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize connections
        self.redis_client = None
        self.influx_client = None
        self.setup_connections()
        
    def setup_connections(self):
        """Initialize database connections"""
        try:
            # Redis connection
            if self.config.get('redis_url'):
                self.redis_client = redis.from_url(self.config['redis_url'])
                self.redis_client.ping()
                self.logger.info("✅ Redis connection established")
                
            # InfluxDB connection
            if self.config.get('influx_url'):
                self.influx_client = InfluxDBClient(
                    url=self.config['influx_url'],
                    token=self.config.get('influx_token'),
                    org=self.config.get('influx_org')
                )
                self.logger.info("✅ InfluxDB connection established")
                
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
    
    def get_system_metrics(self) -> Dict:
        """Collect system performance metrics"""
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            cpu_freq = psutil.cpu_freq()
            
            # Memory metrics
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            # Disk metrics
            disk_usage = psutil.disk_usage('/')
            disk_io = psutil.disk_io_counters()
            
            # Network metrics
            network_io = psutil.net_io_counters()
            
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'cpu': {
                    'percent': cpu_percent,
                    'count': cpu_count,
                    'frequency_mhz': cpu_freq.current if cpu_freq else None
                },
                'memory': {
                    'total_gb': round(memory.total / (1024**3), 2),
                    'used_gb': round(memory.used / (1024**3), 2),
                    'available_gb': round(memory.available / (1024**3), 2),
                    'percent': memory.percent
                },
                'swap': {
                    'total_gb': round(swap.total / (1024**3), 2),
                    'used_gb': round(swap.used / (1024**3), 2),
                    'percent': swap.percent
                },
                'disk': {
                    'total_gb': round(disk_usage.total / (1024**3), 2),
                    'used_gb': round(disk_usage.used / (1024**3), 2),
                    'free_gb': round(disk_usage.free / (1024**3), 2),
                    'percent': (disk_usage.used / disk_usage.total) * 100,
                    'read_mb': round(disk_io.read_bytes / (1024**2), 2) if disk_io else 0,
                    'write_mb': round(disk_io.write_bytes / (1024**2), 2) if disk_io else 0
                },
                'network': {
                    'bytes_sent_mb': round(network_io.bytes_sent / (1024**2), 2),
                    'bytes_recv_mb': round(network_io.bytes_recv / (1024**2), 2),
                    'packets_sent': network_io.packets_sent,
                    'packets_recv': network_io.packets_recv
                }
            }
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {e}")
            return {}
    
    def get_application_metrics(self) -> Dict:
        """Collect application-specific metrics"""
        metrics = {}
        
        try:
            # FastAPI health check
            if self.config.get('api_url'):
                response = requests.get(
                    f"{self.config['api_url']}/health/websocket", 
                    timeout=5
                )
                if response.status_code == 200:
                    app_health = response.json()
                    metrics['api_health'] = app_health
                    metrics['api_response_time_ms'] = response.elapsed.total_seconds() * 1000
                else:
                    metrics['api_health'] = {'status': 'unhealthy', 'code': response.status_code}
        except Exception as e:
            metrics['api_health'] = {'status': 'error', 'message': str(e)}
        
        return metrics
    
    def get_database_metrics(self) -> Dict:
        """Collect database performance metrics"""
        metrics = {}
        
        try:
            # Redis metrics
            if self.redis_client:
                redis_info = self.redis_client.info()
                metrics['redis'] = {
                    'connected_clients': redis_info.get('connected_clients', 0),
                    'used_memory_mb': round(redis_info.get('used_memory', 0) / (1024**2), 2),
                    'keyspace_hits': redis_info.get('keyspace_hits', 0),
                    'keyspace_misses': redis_info.get('keyspace_misses', 0),
                    'total_commands_processed': redis_info.get('total_commands_processed', 0),
                    'instantaneous_ops_per_sec': redis_info.get('instantaneous_ops_per_sec', 0)
                }
                
                # Calculate hit ratio
                hits = redis_info.get('keyspace_hits', 0)
                misses = redis_info.get('keyspace_misses', 0)
                if hits + misses > 0:
                    metrics['redis']['hit_ratio'] = round(hits / (hits + misses), 4)
                    
        except Exception as e:
            self.logger.error(f"Error collecting Redis metrics: {e}")
            metrics['redis'] = {'status': 'error', 'message': str(e)}
        
        try:
            # InfluxDB metrics (basic health check)
            if self.influx_client:
                health = self.influx_client.health()
                metrics['influxdb'] = {
                    'status': health.status,
                    'message': health.message if hasattr(health, 'message') else 'healthy'
                }
        except Exception as e:
            self.logger.error(f"Error collecting InfluxDB metrics: {e}")
            metrics['influxdb'] = {'status': 'error', 'message': str(e)}
        
        return metrics
    
    def get_process_metrics(self) -> Dict:
        """Get metrics for specific processes"""
        metrics = {}
        
        try:
            # Find Python processes (your application)
            python_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_info']):
                if 'python' in proc.info['name'].lower():
                    python_processes.append({
                        'pid': proc.info['pid'],
                        'name': proc.info['name'],
                        'cpu_percent': proc.info['cpu_percent'],
                        'memory_mb': round(proc.info['memory_info'].rss / (1024**2), 2)
                    })
            
            metrics['python_processes'] = python_processes
            
            # Total resource usage by Python processes
            total_cpu = sum(p['cpu_percent'] for p in python_processes)
            total_memory = sum(p['memory_mb'] for p in python_processes)
            
            metrics['python_total'] = {
                'process_count': len(python_processes),
                'total_cpu_percent': round(total_cpu, 2),
                'total_memory_mb': round(total_memory, 2)
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting process metrics: {e}")
            
        return metrics
    
    def collect_all_metrics(self) -> Dict:
        """Collect all performance metrics"""
        timestamp = datetime.now(timezone.utc)
        
        metrics = {
            'timestamp': timestamp.isoformat(),
            'elapsed_seconds': (timestamp - self.start_time).total_seconds() if self.start_time else 0,
            'system': self.get_system_metrics(),
            'application': self.get_application_metrics(),
            'database': self.get_database_metrics(),
            'processes': self.get_process_metrics()
        }
        
        return metrics
    
    def save_metrics_to_csv(self, filename: str = None):
        """Save collected metrics to CSV file"""
        if not filename:
            filename = f"stress_test_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        if not self.metrics_data:
            self.logger.warning("No metrics data to save")
            return
        
        # Flatten the nested dictionary for CSV export
        flattened_data = []
        for metric in self.metrics_data:
            flat_row = self.flatten_dict(metric)
            flattened_data.append(flat_row)
        
        # Write to CSV
        if flattened_data:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = flattened_data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_data)
            
            self.logger.info(f"✅ Metrics saved to {filename}")
    
    def flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Flatten nested dictionary for CSV export"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    def print_real_time_summary(self, metrics: Dict):
        """Print real-time metrics summary"""
        system = metrics.get('system', {})
        
        # Clear screen for real-time display
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print("🔥 STRESS TEST MONITORING - REAL TIME")
        print("=" * 60)
        print(f"⏱️  Elapsed Time: {metrics.get('elapsed_seconds', 0):.1f} seconds")
        print(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # System metrics
        if system:
            print("💻 SYSTEM PERFORMANCE:")
            print(f"   CPU Usage: {system.get('cpu', {}).get('percent', 0):.1f}%")
            print(f"   Memory Usage: {system.get('memory', {}).get('percent', 0):.1f}% "
                  f"({system.get('memory', {}).get('used_gb', 0):.1f}GB used)")
            print(f"   Disk Usage: {system.get('disk', {}).get('percent', 0):.1f}%")
            print()
        
        # Application metrics
        app = metrics.get('application', {})
        if app.get('api_health'):
            api_status = app['api_health'].get('status', 'unknown')
            response_time = app.get('api_response_time_ms', 0)
            print("🚀 APPLICATION STATUS:")
            print(f"   API Health: {api_status}")
            print(f"   Response Time: {response_time:.1f}ms")
            print()
        
        # Database metrics
        db = metrics.get('database', {})
        if db.get('redis'):
            redis_metrics = db['redis']
            print("📊 DATABASE PERFORMANCE:")
            print(f"   Redis Clients: {redis_metrics.get('connected_clients', 0)}")
            print(f"   Redis Memory: {redis_metrics.get('used_memory_mb', 0):.1f}MB")
            print(f"   Redis Ops/sec: {redis_metrics.get('instantaneous_ops_per_sec', 0)}")
            print(f"   Redis Hit Ratio: {redis_metrics.get('hit_ratio', 0):.2%}")
            print()
        
        # Process metrics
        processes = metrics.get('processes', {})
        if processes.get('python_total'):
            py_metrics = processes['python_total']
            print("🐍 PYTHON PROCESSES:")
            print(f"   Process Count: {py_metrics.get('process_count', 0)}")
            print(f"   Total CPU: {py_metrics.get('total_cpu_percent', 0):.1f}%")
            print(f"   Total Memory: {py_metrics.get('total_memory_mb', 0):.1f}MB")
            print()
        
        print("=" * 60)
        print("Press Ctrl+C to stop monitoring")
    
    def start_monitoring(self, interval: int = 5, save_interval: int = 60):
        """Start the monitoring process"""
        self.monitoring = True
        self.start_time = datetime.now(timezone.utc)
        self.logger.info("🚀 Starting stress test monitoring...")
        
        last_save_time = time.time()
        
        try:
            while self.monitoring:
                # Collect metrics
                metrics = self.collect_all_metrics()
                self.metrics_data.append(metrics)
                
                # Print real-time summary
                self.print_real_time_summary(metrics)
                
                # Save periodically
                current_time = time.time()
                if current_time - last_save_time >= save_interval:
                    self.save_metrics_to_csv()
                    last_save_time = current_time
                
                # Wait for next interval
                time.sleep(interval)
                
        except KeyboardInterrupt:
            self.logger.info("🛑 Monitoring stopped by user")
        finally:
            self.stop_monitoring()
    
    def stop_monitoring(self):
        """Stop monitoring and save final results"""
        self.monitoring = False
        
        if self.metrics_data:
            # Save final metrics
            self.save_metrics_to_csv()
            
            # Generate summary report
            self.generate_summary_report()
        
        self.logger.info("✅ Monitoring stopped and data saved")
    
    def generate_summary_report(self):
        """Generate a summary report of the stress test"""
        if not self.metrics_data:
            return
        
        report_filename = f"stress_test_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_filename, 'w') as f:
            f.write("STRESS TEST SUMMARY REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            # Test duration
            duration = self.metrics_data[-1]['elapsed_seconds']
            f.write(f"Test Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)\n")
            f.write(f"Data Points Collected: {len(self.metrics_data)}\n\n")
            
            # CPU statistics
            cpu_values = [m['system'].get('cpu', {}).get('percent', 0) for m in self.metrics_data if m.get('system')]
            if cpu_values:
                f.write("CPU USAGE STATISTICS:\n")
                f.write(f"  Average: {sum(cpu_values)/len(cpu_values):.1f}%\n")
                f.write(f"  Maximum: {max(cpu_values):.1f}%\n")
                f.write(f"  Minimum: {min(cpu_values):.1f}%\n\n")
            
            # Memory statistics
            memory_values = [m['system'].get('memory', {}).get('percent', 0) for m in self.metrics_data if m.get('system')]
            if memory_values:
                f.write("MEMORY USAGE STATISTICS:\n")
                f.write(f"  Average: {sum(memory_values)/len(memory_values):.1f}%\n")
                f.write(f"  Maximum: {max(memory_values):.1f}%\n")
                f.write(f"  Minimum: {min(memory_values):.1f}%\n\n")
            
            # API response times
            response_times = [m['application'].get('api_response_time_ms', 0) for m in self.metrics_data if m.get('application')]
            response_times = [rt for rt in response_times if rt > 0]  # Filter out zeros
            if response_times:
                f.write("API RESPONSE TIME STATISTICS:\n")
                f.write(f"  Average: {sum(response_times)/len(response_times):.1f}ms\n")
                f.write(f"  Maximum: {max(response_times):.1f}ms\n")
                f.write(f"  Minimum: {min(response_times):.1f}ms\n\n")
        
        self.logger.info(f"📊 Summary report saved to {report_filename}")


# Example configuration
def get_config():
    """Get configuration for monitoring"""
    return {
        'api_url': 'http://localhost:8000',  # Your FastAPI application URL
        'redis_url': 'redis://localhost:6379/0',  # Your Redis URL
        'influx_url': 'http://65.1.181.184:8086',  # Your InfluxDB URL
        'influx_token': 'qGkyieT5SGjZ84U-El40zMoYa9LWmHBSVVpSC0d4z2MBcu5OSSCFmZo-Z1QM3yduCIJMtd30EOPIa6KIVG5klA==',
        'influx_org': 'chaitanya-org'
    }


if __name__ == "__main__":
    config = get_config()
    monitor = StressTestMonitor(config)
    
    print("🔥 Stress Test Performance Monitor")
    print("This will monitor system resources, application metrics, and database performance")
    print("Make sure your application is running before starting the stress test")
    print()
    
    try:
        # Start monitoring with 5-second intervals
        monitor.start_monitoring(interval=5, save_interval=60)
    except Exception as e:
        print(f"❌ Monitoring failed: {e}")
        logging.error(f"Monitoring error: {e}")