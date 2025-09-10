#!/usr/bin/env python3
# Copyright 2025 Universitat Politècnica de València and contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cluster manager for handling multiple OSCAR cluster connections."""

import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from threading import Lock

log = logging.getLogger("oscar-backend")


@dataclass
class ClusterConfig:
    """Configuration for a single OSCAR cluster."""
    endpoint: str
    token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    ssl: bool = True
    name: Optional[str] = None
    steps: Optional[List[str]] = None
    
    def __post_init__(self):
        """Validate cluster configuration."""
        if not self.endpoint:
            raise ValueError("Cluster endpoint is required")
        
        if not self.token and not (self.username and self.password):
            raise ValueError("Either token or username/password must be provided")
        
        if self.username and not self.password:
            raise ValueError("Password is required when username is provided")
        
        # Generate a name if not provided
        if not self.name:
            self.name = f"cluster-{self.endpoint.split('://')[-1].split('/')[0]}"
            
        # Initialize steps as empty list if None
        if self.steps is None:
            self.steps = []


class ClusterManager:
    """Manages multiple OSCAR cluster connections with round-robin scheduling."""
    
    def __init__(self):
        self.clusters: List[ClusterConfig] = []
        self.current_cluster_index = 0
        self._lock = Lock()
        self._cluster_clients = {}  # Cache for OSCAR clients
        
    def add_cluster(self, config: ClusterConfig) -> None:
        """Add a cluster configuration."""
        log.info("Adding cluster: %s", config.name)
        self.clusters.append(config)
        
    def add_cluster_from_args(self, endpoint: str, token: Optional[str] = None,
                             username: Optional[str] = None, password: Optional[str] = None,
                             ssl: bool = True, steps: Optional[List[str]] = None) -> None:
        """Add a cluster from individual arguments."""
        config = ClusterConfig(
            endpoint=endpoint,
            token=token,
            username=username,
            password=password,
            ssl=ssl,
            steps=steps
        )
        self.add_cluster(config)
        
    def get_next_cluster(self) -> Optional[ClusterConfig]:
        """Get the next cluster using round-robin scheduling."""
        if not self.clusters:
            return None
            
        with self._lock:
            cluster = self.clusters[self.current_cluster_index]
            self.current_cluster_index = (self.current_cluster_index + 1) % len(self.clusters)
            log.debug("Selected cluster: %s (index: %d)", cluster.name, self.current_cluster_index)
            return cluster
            
    def get_cluster_by_name(self, name: str) -> Optional[ClusterConfig]:
        """Get a specific cluster by name."""
        for cluster in self.clusters:
            if cluster.name == name:
                return cluster
        return None
        
    def get_cluster_for_step(self, step_name: str) -> Optional[ClusterConfig]:
        """Get the cluster assigned to a specific workflow step, or use round-robin if not mapped."""
        if not self.clusters:
            return None
            
        # First, check if any cluster has this step explicitly mapped
        for cluster in self.clusters:
            if cluster.steps and step_name in cluster.steps:
                log.info("Step '%s' mapped to cluster '%s'", step_name, cluster.name)
                return cluster
        
        # If no explicit mapping found, use round-robin scheduling
        log.debug("Step '%s' not explicitly mapped, using round-robin selection", step_name)
        return self.get_next_cluster()
        
    def get_cluster_count(self) -> int:
        """Get the total number of clusters."""
        return len(self.clusters)
        
    def validate_clusters(self) -> bool:
        """Validate that all clusters have valid configurations."""
        if not self.clusters:
            log.error("No clusters configured")
            return False
            
        for cluster in self.clusters:
            try:
                # This will raise ValueError if config is invalid
                _ = ClusterConfig(
                    endpoint=cluster.endpoint,
                    token=cluster.token,
                    username=cluster.username,
                    password=cluster.password,
                    ssl=cluster.ssl,
                    name=cluster.name
                )
            except ValueError as e:
                log.error("Invalid cluster configuration for %s: %s", cluster.name, e)
                return False
                
        log.info("Validated %d cluster configurations", len(self.clusters))
        return True
        
    def get_cluster_info(self) -> List[Dict[str, Any]]:
        """Get information about all clusters."""
        info = []
        for i, cluster in enumerate(self.clusters):
            cluster_info = {
                'index': i,
                'name': cluster.name,
                'endpoint': cluster.endpoint,
                'auth_type': 'token' if cluster.token else 'username/password',
                'ssl': cluster.ssl,
                'steps': cluster.steps if cluster.steps else []
            }
            info.append(cluster_info)
        return info
        
    def clear_clusters(self) -> None:
        """Clear all cluster configurations."""
        with self._lock:
            self.clusters.clear()
            self.current_cluster_index = 0
            self._cluster_clients.clear()
            
    def __len__(self) -> int:
        """Return the number of clusters."""
        return len(self.clusters)
        
    def __iter__(self):
        """Iterate over cluster configurations."""
        return iter(self.clusters)
