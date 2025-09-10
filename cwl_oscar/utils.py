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

"""Utility functions for cwl-oscar."""

import logging
from typing import Dict, Optional

from oscar_python.client import Client

try:
    from .constants import *
except ImportError:
    # Fallback for standalone execution
    from constants import *

log = logging.getLogger("oscar-backend")


def create_oscar_client_options(
    endpoint: str,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    ssl: bool = True,
    cluster_id: str = DEFAULT_CLUSTER_ID
) -> Dict[str, str]:
    """
    Create OSCAR client options dictionary based on authentication method.
    
    Args:
        endpoint: OSCAR cluster endpoint URL
        token: OIDC authentication token
        username: Username for basic authentication
        password: Password for basic authentication
        ssl: Enable SSL/TLS for OSCAR communication
        cluster_id: OSCAR cluster ID
        
    Returns:
        Dictionary with OSCAR client options
        
    Raises:
        ValueError: If neither token nor username/password are provided
    """
    if token:
        log.debug("Using OIDC token authentication")
        options = {
            'cluster_id': cluster_id,
            'endpoint': endpoint,
            'oidc_token': token,
            'ssl': str(ssl)
        }
    elif username and password:
        log.debug("Using username/password authentication")
        options = {
            'cluster_id': cluster_id,
            'endpoint': endpoint,
            'user': username,
            'password': password,
            'ssl': str(ssl)
        }
    else:
        raise ValueError("Either OIDC token or username/password must be provided for OSCAR authentication")
    
    return options


def create_oscar_client(
    endpoint: str,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    ssl: bool = True,
    cluster_id: str = DEFAULT_CLUSTER_ID
) -> Client:
    """
    Create and return an OSCAR client with the specified authentication.
    
    Args:
        endpoint: OSCAR cluster endpoint URL
        token: OIDC authentication token
        username: Username for basic authentication
        password: Password for basic authentication
        ssl: Enable SSL/TLS for OSCAR communication
        cluster_id: OSCAR cluster ID
        
    Returns:
        Configured OSCAR client instance
    """
    options = create_oscar_client_options(
        endpoint, token, username, password, ssl, cluster_id
    )
    
    # Log options without sensitive information
    safe_options = {k: '***' if k in ['oidc_token', 'password'] else v for k, v in options.items()}
    log.debug("Creating OSCAR client with options: %s", safe_options)
    
    client = Client(options=options)
    log.debug("OSCAR client created successfully")
    
    return client


def sanitize_service_name(name: str) -> str:
    """
    Sanitize service name to follow Kubernetes naming rules (RFC 1123 subdomain).
    
    Args:
        name: Original service name
        
    Returns:
        Sanitized service name safe for Kubernetes
    """
    import re
    
    # Replace underscores with hyphens and ensure only lowercase alphanumeric + hyphens
    clean_name = name.lower().replace('_', '-')
    # Remove any other invalid characters, keep only a-z, 0-9, and hyphens
    clean_name = re.sub(r'[^a-z0-9-]', '', clean_name)
    # Ensure it doesn't start or end with a hyphen
    clean_name = clean_name.strip('-')
    # Ensure it's not empty
    if not clean_name:
        clean_name = 'tool'
    
    return clean_name
