"""
Gluetun container controller.
Handles Docker container restart for IP rotation on rate limits/blocks.
"""
import os
import time
import logging
from typing import Optional
import docker
import requests

logger = logging.getLogger(__name__)


class GluetunControllerError(Exception):
    """Base exception for Gluetun controller errors."""
    pass


class GluetunController:
    """Controls Gluetun Docker container for IP rotation."""
    
    def __init__(
        self,
        container_name: str = "gluetun",
        restart_cooldown: int = 15,
        max_restart_attempts: int = 5,
        ip_check_url: str = "https://httpbin.org/ip",
        proxy_url: Optional[str] = None
    ):
        """
        Initialize Gluetun controller.
        
        Args:
            container_name: Name of the Gluetun container
            restart_cooldown: Seconds to wait after restart before checking
            max_restart_attempts: Max consecutive restart attempts
            ip_check_url: URL to check current public IP
            proxy_url: Proxy URL to use for IP verification
        """
        self.container_name = container_name
        self.restart_cooldown = restart_cooldown
        self.max_restart_attempts = max_restart_attempts
        self.ip_check_url = ip_check_url
        self.proxy_url = proxy_url
        
        self.docker_client: Optional[docker.DockerClient] = None
        self.current_ip: Optional[str] = None
        self.restart_count = 0
    
    def connect(self) -> None:
        """Connect to Docker daemon."""
        try:
            self.docker_client = docker.from_env()
            self.docker_client.ping()
            logger.info("Connected to Docker daemon")
        except docker.errors.DockerException as e:
            raise GluetunControllerError(f"Failed to connect to Docker: {e}")
    
    def _get_container(self):
        """Get the Gluetun container object."""
        if not self.docker_client:
            self.connect()
        
        try:
            return self.docker_client.containers.get(self.container_name)
        except docker.errors.NotFound:
            raise GluetunControllerError(
                f"Container '{self.container_name}' not found"
            )
    
    def get_current_ip(self) -> Optional[str]:
        """
        Get current public IP through the proxy.
        
        Returns:
            Current public IP address or None on failure
        """
        proxies = {}
        if self.proxy_url:
            proxies = {"http": self.proxy_url, "https": self.proxy_url}
        
        try:
            response = requests.get(
                self.ip_check_url,
                proxies=proxies,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            ip = data.get("origin", "").split(",")[0].strip()
            return ip if ip else None
        except Exception as e:
            logger.warning(f"Failed to get current IP: {e}")
            return None
    
    def verify_proxy_active(self) -> bool:
        """
        Verify that traffic is routing through the proxy.
        
        Returns:
            True if proxy is active and working
        """
        ip = self.get_current_ip()
        if ip:
            self.current_ip = ip
            logger.info(f"Current public IP: {ip}")
            return True
        return False
    
    def wait_for_healthy(self, timeout: int = 60) -> bool:
        """
        Wait for Gluetun VPN connection to be ready.
        
        Args:
            timeout: Maximum seconds to wait
            
        Returns:
            True if VPN is connected and proxy is working
        """
        container = self._get_container()
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            container.reload()
            status = container.status
            
            if status != "running":
                logger.debug(f"Container status: {status}, waiting...")
                time.sleep(2)
                continue
            
            # Check if proxy is actually working by testing connection
            ip = self.get_current_ip()
            if ip:
                logger.info(f"VPN connected, IP: {ip}")
                return True
            
            logger.debug("Waiting for VPN connection...")
            time.sleep(3)
        
        return False
    
    def restart_for_new_ip(self) -> bool:
        """
        Restart Gluetun container to obtain a new IP address.
        
        Returns:
            True if restart successful and new IP obtained
        """
        if self.restart_count >= self.max_restart_attempts:
            raise GluetunControllerError(
                f"Max restart attempts ({self.max_restart_attempts}) exceeded"
            )
        
        container = self._get_container()
        old_ip = self.current_ip
        
        logger.info(f"Restarting Gluetun container (attempt {self.restart_count + 1})")
        
        try:
            container.restart(timeout=30)
            self.restart_count += 1
            
            # Wait for container to be healthy
            logger.info(f"Waiting {self.restart_cooldown}s for Gluetun to reconnect...")
            time.sleep(self.restart_cooldown)
            
            if not self.wait_for_healthy(timeout=60):
                logger.error("Gluetun container failed to become healthy")
                return False
            
            # Verify we got a new IP
            new_ip = self.get_current_ip()
            if not new_ip:
                logger.error("Failed to verify new IP after restart")
                return False
            
            self.current_ip = new_ip
            
            if new_ip == old_ip:
                logger.warning(f"IP unchanged after restart: {new_ip}")
                # Still consider it a success, VPN might have same exit server
            else:
                logger.info(f"New IP obtained: {old_ip} -> {new_ip}")
                self.restart_count = 0  # Reset counter on successful IP change
            
            return True
            
        except docker.errors.APIError as e:
            logger.error(f"Failed to restart container: {e}")
            return False
    
    def reset_restart_counter(self) -> None:
        """Reset the restart attempt counter."""
        self.restart_count = 0


def create_controller_from_env() -> GluetunController:
    """Create GluetunController from environment variables."""
    return GluetunController(
        container_name=os.environ.get("GLUETUN_CONTAINER_NAME", "gluetun"),
        restart_cooldown=int(os.environ.get("GLUETUN_RESTART_COOLDOWN", "15")),
        max_restart_attempts=int(os.environ.get("MAX_RESTART_ATTEMPTS", "5")),
        ip_check_url=os.environ.get("IP_CHECK_URL", "https://httpbin.org/ip"),
        proxy_url=os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    )
