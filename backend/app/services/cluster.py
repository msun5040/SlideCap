"""
GPU cluster integration via SSH + tmux (no Slurm).

Handles direct SSH connection with per-session credentials,
GPU status queries, file transfer (rsync/SFTP), tmux job management,
and background status polling.
"""
import json
import shutil
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import paramiko

from ..db import get_session, AnalysisJob, JobSlide, Analysis, Slide


class ClusterService:
    """Manage SSH connection and job execution on a GPU cluster."""

    def __init__(self, host: Optional[str] = None, port: int = 22):
        self._host = host
        self._port = port
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._client: Optional[paramiko.SSHClient] = None
        self._lock = threading.Lock()

    def connect(self, host: str, port: int, username: str, password: str) -> dict:
        """
        Open SSH connection with provided credentials.
        Returns {"connected": True, "message": ...} or raises.
        """
        with self._lock:
            # Close existing connection if any
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    pass
                self._client = None

            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                client.connect(
                    hostname=host,
                    port=port,
                    username=username,
                    password=password,
                    timeout=30,
                    allow_agent=False,
                    look_for_keys=False,
                )
            except Exception as e:
                client.close()
                raise RuntimeError(f"SSH connection failed: {e}")

            self._client = client
            self._host = host
            self._port = port
            self._username = username
            self._password = password

        return {"connected": True, "message": f"Connected to {host} as {username}"}

    def disconnect(self):
        """Close SSH connection."""
        with self._lock:
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    pass
                self._client = None
            self._username = None
            self._password = None

    @property
    def is_connected(self) -> bool:
        with self._lock:
            if self._client is None:
                return False
            transport = self._client.get_transport()
            return transport is not None and transport.is_active()

    @property
    def connection_info(self) -> dict:
        return {
            "connected": self.is_connected,
            "host": self._host,
            "port": self._port,
            "username": self._username,
        }

    def _get_client(self) -> paramiko.SSHClient:
        """Get the SSH client, reconnecting if needed."""
        with self._lock:
            if self._client is not None:
                transport = self._client.get_transport()
                if transport is not None and transport.is_active():
                    return self._client
                # Dead connection — try reconnect
                try:
                    self._client.close()
                except Exception:
                    pass

            if not self._host or not self._username or not self._password:
                raise RuntimeError("Not connected to cluster. Please connect first.")

            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=self._host,
                port=self._port,
                username=self._username,
                password=self._password,
                timeout=30,
                allow_agent=False,
                look_for_keys=False,
            )
            self._client = client
            return client

    def run_command(self, command: str, timeout: int = 60) -> tuple[str, str, int]:
        """Run a command via SSH. Returns (stdout, stderr, exit_code)."""
        client = self._get_client()
        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        return stdout.read().decode().strip(), stderr.read().decode().strip(), exit_code

    def get_gpu_status(self) -> list[dict]:
        """
        Query nvidia-smi for GPU status.
        Returns list of {index, name, memory_used_mb, memory_total_mb, utilization_pct}.
        """
        query = (
            "--query-gpu=index,name,memory.used,memory.total,utilization.gpu "
            "--format=csv,noheader,nounits"
        )
        # Try common nvidia-smi locations
        stdout, exit_code = "", 1
        for nvidia_smi in ["nvidia-smi", "/usr/bin/nvidia-smi", "/usr/local/cuda/bin/nvidia-smi"]:
            stdout, stderr, exit_code = self.run_command(f"{nvidia_smi} {query}")
            if exit_code == 0:
                break

        if exit_code != 0:
            # No GPUs available (CPU-only node) — not an error
            return []

        gpus = []
        for line in stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 5:
                gpus.append({
                    "index": int(parts[0]),
                    "name": parts[1],
                    "memory_used_mb": int(parts[2]),
                    "memory_total_mb": int(parts[3]),
                    "utilization_pct": int(parts[4]),
                })
        return gpus

    def rsync_slide(self, local_path: Path, remote_dir: str) -> str:
        """
        Transfer a slide file to the cluster.
        Uses rsync with sshpass if available, falls back to paramiko SFTP.
        Returns the remote file path.
        """
        filename = local_path.name
        remote_path = f"{remote_dir}/{filename}"

        # Ensure remote directory exists
        self.run_command(f"mkdir -p {remote_dir}")

        # Try rsync + sshpass first (faster for large files)
        if shutil.which("sshpass") and shutil.which("rsync"):
            try:
                cmd = [
                    "sshpass", "-p", self._password,
                    "rsync", "-avzP", "--no-perms",
                    "-e", f"ssh -p {self._port} -o StrictHostKeyChecking=no",
                    str(local_path),
                    f"{self._username}@{self._host}:{remote_dir}/",
                ]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=3600,  # 1 hour timeout for large files
                )
                if result.returncode == 0:
                    return remote_path
                print(f"[Cluster] rsync failed: {result.stderr}, falling back to SFTP")
            except Exception as e:
                print(f"[Cluster] rsync error: {e}, falling back to SFTP")

        # Fallback: paramiko SFTP
        client = self._get_client()
        sftp = client.open_sftp()
        try:
            sftp.put(str(local_path), remote_path)
        finally:
            sftp.close()

        return remote_path

    def start_job(
        self,
        analysis: Analysis,
        slide_hash: str,
        remote_wsi_path: str,
        remote_output_dir: str,
        gpu_index: int,
        parameters: Optional[dict] = None,
    ) -> str:
        """
        Start an analysis job in a tmux session.
        Returns the tmux session name.
        """
        session_name = f"slidecap_{analysis.name.lower().replace(' ', '_')}_{slide_hash[:8]}"

        # Build the command from template
        # GPU note: CUDA_VISIBLE_DEVICES is set to the real gpu_index,
        # which makes it appear as device 0 inside the process.
        # So {gpu} in the template should always be 0.
        params = parameters or {}
        template_vars = {
            "wsi_path": remote_wsi_path,
            "wsi_dir": str(Path(remote_wsi_path).parent),
            "outdir": remote_output_dir,
            "gpu": "0",  # Always 0 because CUDA_VISIBLE_DEVICES handles the real mapping
            "batch_size": str(params.get("batch_size", 4)),
            "model_path": params.get("model_path", ""),
        }

        # Use command_template if available, otherwise construct from script_path
        if analysis.command_template:
            command = analysis.command_template.format(**template_vars)
        elif analysis.script_path:
            command = f"{analysis.script_path} {remote_wsi_path} {remote_output_dir}"
        else:
            raise RuntimeError("Analysis has no command_template or script_path defined")

        # Build the full tmux command
        parts = []
        if analysis.working_directory:
            parts.append(f"cd {analysis.working_directory}")
        if analysis.env_setup:
            parts.append(analysis.env_setup)

        if analysis.gpu_required:
            parts.append(f"export CUDA_VISIBLE_DEVICES={gpu_index}")
        parts.append(f"mkdir -p {remote_output_dir}")
        parts.append(f"{command} 2>&1 | tee {remote_output_dir}/run.log")

        full_command = " && ".join(parts)

        # Create tmux session
        tmux_cmd = f"tmux new-session -d -s {session_name} '{full_command}'"
        stdout, stderr, exit_code = self.run_command(tmux_cmd)
        if exit_code != 0:
            raise RuntimeError(f"tmux session creation failed: {stderr}")

        return session_name

    def check_job_status(self, session_name: str, remote_output_dir: str) -> dict:
        """
        Check if a tmux session is alive and read log tail.
        Returns {"alive": bool, "log_tail": str}.
        """
        # Check if tmux session exists
        _, _, exit_code = self.run_command(f"tmux has-session -t {session_name} 2>/dev/null")
        alive = exit_code == 0

        # Read last 50 lines of run.log
        log_tail = ""
        if remote_output_dir:
            stdout, _, _ = self.run_command(f"tail -50 {remote_output_dir}/run.log 2>/dev/null")
            log_tail = stdout

        return {"alive": alive, "log_tail": log_tail}

    def cancel_job(self, session_name: str) -> bool:
        """Kill a tmux session."""
        _, _, exit_code = self.run_command(f"tmux kill-session -t {session_name} 2>/dev/null")
        return exit_code == 0

    def list_tmux_sessions(self) -> list[str]:
        """List all slidecap tmux sessions."""
        stdout, _, exit_code = self.run_command("tmux list-sessions -F '#{session_name}' 2>/dev/null")
        if exit_code != 0 or not stdout:
            return []
        return [s for s in stdout.split("\n") if s.startswith("slidecap_")]


class JobStatusPoller:
    """Daemon thread that polls cluster for job status updates."""

    def __init__(self, cluster: ClusterService, interval: int = 30,
                 indexer=None, analyses_path: Optional[Path] = None):
        self.cluster = cluster
        self.interval = interval
        self.indexer = indexer
        self.analyses_path = analyses_path
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        print(f"[Poller] Started (interval={self.interval}s)")

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        print("[Poller] Stopped")

    def poll_now(self):
        """Trigger an immediate status check."""
        self._do_poll()

    def _poll_loop(self):
        while not self._stop_event.is_set():
            try:
                if self.cluster.is_connected:
                    self._do_poll()
            except Exception as e:
                print(f"[Poller] Error: {e}")
            self._stop_event.wait(self.interval)

    def _do_poll(self):
        if not self.cluster.is_connected:
            return

        db = get_session()
        try:
            # Get all active job_slides with cluster IDs (tmux session names)
            active_slides = (
                db.query(JobSlide)
                .filter(
                    JobSlide.status.in_(["queued", "running", "transferring"]),
                    JobSlide.cluster_job_id.isnot(None),
                )
                .all()
            )

            if not active_slides:
                return

            updated = 0
            affected_job_ids = set()

            for js in active_slides:
                try:
                    info = self.cluster.check_job_status(
                        js.cluster_job_id,
                        js.remote_output_path or ""
                    )
                except Exception as e:
                    print(f"[Poller] Failed to check job_slide {js.id}: {e}")
                    continue

                # Update log tail
                if info.get("log_tail"):
                    js.log_tail = info["log_tail"]

                if info["alive"]:
                    if js.status != "running":
                        js.status = "running"
                        if not js.started_at:
                            js.started_at = datetime.utcnow()
                        updated += 1
                        affected_job_ids.add(js.job_id)
                else:
                    # Session gone — check if completed or failed
                    if js.status in ("queued", "running"):
                        output_dir = js.remote_output_path
                        if output_dir:
                            stdout, _, _ = self.cluster.run_command(
                                f"ls {output_dir}/ 2>/dev/null | head -20"
                            )
                            has_output = bool(stdout.strip())
                        else:
                            has_output = False

                        if has_output:
                            js.status = "completed"
                            # Auto-transfer results back to network drive
                            local_path = self._transfer_results(js)
                            if local_path:
                                js.local_output_path = local_path
                        else:
                            js.status = "failed"
                            if not js.error_message:
                                js.error_message = "tmux session ended without output files"

                        js.completed_at = datetime.utcnow()
                        updated += 1
                        affected_job_ids.add(js.job_id)

            # Recompute parent job statuses
            if affected_job_ids:
                for job_id in affected_job_ids:
                    job = db.query(AnalysisJob).filter_by(id=job_id).first()
                    if job:
                        self._recompute_job_status(job)

            if updated:
                db.commit()
                print(f"[Poller] Updated {updated} slide(s) across {len(affected_job_ids)} job(s)")
            else:
                db.commit()  # Commit log_tail updates

        except Exception as e:
            db.rollback()
            raise
        finally:
            db.close()

    def _transfer_results(self, js: JobSlide) -> Optional[str]:
        """Rsync results from cluster back to network drive for a completed slide."""
        if not self.indexer or not self.analyses_path or not js.remote_output_path:
            return None

        slide = js.slide
        if not slide:
            return None

        # Get original filename stem for filtering per-slide files
        filepath = self.indexer.get_filepath(slide.slide_hash)
        if not filepath:
            print(f"[Poller] No filepath for slide {slide.slide_hash[:12]}, skipping transfer")
            return None

        filename_stem = filepath.stem
        analysis_name = js.job.model_name if js.job else "unknown"
        local_dir = self.analyses_path / slide.slide_hash / analysis_name
        local_dir.mkdir(parents=True, exist_ok=True)

        remote_path = js.remote_output_path.rstrip("/") + "/"

        # Rsync per-slide files (matching filename stem) + shared log files
        if not (shutil.which("sshpass") and shutil.which("rsync")):
            print(f"[Poller] sshpass/rsync not available, skipping transfer for {slide.slide_hash[:12]}")
            return None

        try:
            cmd = [
                "sshpass", "-p", self.cluster._password,
                "rsync", "-avz", "--no-perms",
                "-e", f"ssh -p {self.cluster._port} -o StrictHostKeyChecking=no",
                "--include", f"{filename_stem}*",
                "--include", "run.log",
                "--include", "filelist.csv",
                "--include", "processed.log",
                "--include", "progress.log",
                "--exclude", "*",
                f"{self.cluster._username}@{self.cluster._host}:{remote_path}",
                str(local_dir) + "/",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode == 0:
                print(f"[Poller] Transferred results for {slide.slide_hash[:12]} -> {local_dir}")
                return str(local_dir)
            else:
                print(f"[Poller] Rsync failed for {slide.slide_hash[:12]}: {result.stderr[:200]}")
                return None
        except Exception as e:
            print(f"[Poller] Transfer error for {slide.slide_hash[:12]}: {e}")
            return None

    @staticmethod
    def _recompute_job_status(job: AnalysisJob):
        """Derive parent job status from its child JobSlides."""
        if not job.slides:
            return
        statuses = [js.status for js in job.slides]
        if any(s in ("running", "transferring") for s in statuses):
            job.status = "running"
            if not job.started_at:
                job.started_at = datetime.utcnow()
        elif all(s == "completed" for s in statuses):
            job.status = "completed"
            if not job.completed_at:
                job.completed_at = datetime.utcnow()
        elif any(s == "failed" for s in statuses) and not any(s in ("running", "transferring", "pending") for s in statuses):
            job.status = "failed"
            if not job.completed_at:
                job.completed_at = datetime.utcnow()
        elif all(s == "pending" for s in statuses):
            job.status = "pending"
