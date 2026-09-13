import os
import logging
from termcolor import colored

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_pod_memory_usage() -> float:
    """
    Returns the current memory usage of the Kubernetes Pod in Gigabytes (GB).
    """
    # Path for cgroups v2 (Modern Kubernetes)
    cg2_path = "/sys/fs/cgroup/memory.current"
    # Path for cgroups v1 (Older Kubernetes)
    cg1_path = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
    
    try:
        if os.path.exists(cg2_path):
            with open(cg2_path, "r") as f:
                bytes_used = int(f.read().strip())
        elif os.path.exists(cg1_path):
            with open(cg1_path, "r") as f:
                bytes_used = int(f.read().strip())
        else:
            raise FileNotFoundError("Could not find cgroup memory metrics path.")
            
        current_usage = bytes_used / (1024 ** 2)
        percent_used = (current_usage / (1024 * 30.0)) * 100
        if percent_used < 60: text_color = 'green'
        elif percent_used < 85: text_color = 'yellow'
        else: text_color = 'red'
        print(colored(f"Pod Memory: {(current_usage / 1024):.2f} GB / 30 GB ({percent_used:.1f}% used)", text_color))
        # Convert bytes to Gigabytes (GB)
        return current_usage
        
    except Exception as e:
        logging.error(f"Failed to read cgroup memory: {e}")
        return 0.0
