"""
Slurm GPU cluster integration via SSH (paramiko).

Handles sbatch submission, job status polling, and cancellation.
"""
import json
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import paramiko

from ..config import settings
from ..db import get_session, AnalysisJob, Analysis, Slide


class SlurmService:
    """Submit and manage Slurm jobs via SSH."""

    def __init__(self):
        self._client: Optional[paramiko.SSHClient] = None
        self._lock = threading.Lock()

    def _get_client(self) -> paramiko.SSHClient:
        """Get or create an SSH connection with auto-reconnect."""
        with self._lock:
            if self._client is not None:
                # Test if connection is still alive
                transport = self._client.get_transport()
                if transport is not None and transport.is_active():
                    return self._client
                # Connection dead — reconnect
                try:
                    self._client.close()
                except Exception:
                    pass

            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=settings.SSH_HOST,
                port=settings.SSH_PORT,
                username=settings.SSH_USER,
                key_filename=settings.SSH_KEY_PATH,
                timeout=30,
            )
            self._client = client
            return client

    def _run_command(self, command: str) -> tuple[str, str, int]:
        """Run a command via SSH and return (stdout, stderr, exit_code)."""
        client = self._get_client()
        stdin, stdout, stderr = client.exec_command(command, timeout=60)
        exit_code = stdout.channel.recv_exit_status()
        return stdout.read().decode().strip(), stderr.read().decode().strip(), exit_code

    def submit_job(
        self,
        analysis: Analysis,
        slide: Slide,
        slide_path: Path,
        parameters: Optional[dict] = None,
    ) -> str:
        """
        Generate and submit an sbatch script. Returns the Slurm job ID.
        """
        output_dir = f"{settings.analyses_path}/{slide.slide_hash}/{analysis.name}_v{analysis.version}"
        params_json = json.dumps(parameters or {})

        time_limit = max(analysis.estimated_runtime_minutes * 2, 30)
        hours = time_limit // 60
        minutes = time_limit % 60

        lines = [
            "#!/bin/bash",
            f"#SBATCH --job-name=slidecap_{analysis.name}_{slide.slide_hash[:8]}",
            f"#SBATCH --output={output_dir}/slurm_%j.out",
            f"#SBATCH --error={output_dir}/slurm_%j.err",
            f"#SBATCH --time={hours:02d}:{minutes:02d}:00",
        ]

        if settings.SLURM_PARTITION:
            lines.append(f"#SBATCH --partition={settings.SLURM_PARTITION}")
        if settings.SLURM_ACCOUNT:
            lines.append(f"#SBATCH --account={settings.SLURM_ACCOUNT}")
        if analysis.gpu_required:
            lines.append("#SBATCH --gres=gpu:1")

        lines += [
            "",
            f"mkdir -p {output_dir}",
            "",
            f"singularity exec {'--nv ' if analysis.gpu_required else ''}{analysis.container_image} \\",
            f"  python /app/run.py \\",
            f"    --input {slide_path} \\",
            f"    --output {output_dir} \\",
            f"    --params '{params_json}'",
        ]

        script = "\n".join(lines) + "\n"

        # Write script to a temp file on the cluster and submit
        script_path = f"/tmp/slidecap_sbatch_{slide.slide_hash[:12]}_{analysis.name}.sh"
        # Use heredoc to write the script content
        write_cmd = f"cat > {script_path} << 'SBATCH_EOF'\n{script}SBATCH_EOF"
        self._run_command(write_cmd)

        # Submit
        stdout, stderr, exit_code = self._run_command(f"sbatch {script_path}")
        if exit_code != 0:
            raise RuntimeError(f"sbatch failed (exit {exit_code}): {stderr}")

        # Parse job ID from "Submitted batch job 12345"
        parts = stdout.strip().split()
        if len(parts) >= 4:
            return parts[-1]
        raise RuntimeError(f"Could not parse sbatch output: {stdout}")

    def get_batch_status(self, cluster_job_ids: list[str]) -> dict[str, dict]:
        """
        Query sacct for multiple jobs at once.
        Returns {cluster_job_id: {"state": ..., "start": ..., "end": ..., "error": ...}}
        """
        if not cluster_job_ids:
            return {}

        job_list = ",".join(cluster_job_ids)
        cmd = (
            f"sacct -j {job_list} --format=JobID,State,Start,End,ExitCode "
            f"--noheader --parsable2 --allocations"
        )
        stdout, stderr, exit_code = self._run_command(cmd)

        results = {}
        if exit_code != 0 or not stdout:
            return results

        for line in stdout.strip().split("\n"):
            parts = line.split("|")
            if len(parts) >= 5:
                job_id = parts[0].split(".")[0]  # Strip step suffix
                results[job_id] = {
                    "state": parts[1],
                    "start": parts[2] if parts[2] != "Unknown" else None,
                    "end": parts[3] if parts[3] != "Unknown" else None,
                    "exit_code": parts[4],
                }

        return results

    def cancel_job(self, cluster_job_id: str) -> bool:
        """Cancel a Slurm job."""
        _, stderr, exit_code = self._run_command(f"scancel {cluster_job_id}")
        return exit_code == 0

    def close(self):
        """Close the SSH connection."""
        with self._lock:
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    pass
                self._client = None


# Slurm state → app state mapping
_STATE_MAP = {
    "PENDING": "queued",
    "RUNNING": "running",
    "COMPLETED": "completed",
    "FAILED": "failed",
    "CANCELLED": "failed",
    "TIMEOUT": "failed",
    "OUT_OF_MEMORY": "failed",
    "NODE_FAIL": "failed",
    "PREEMPTED": "queued",  # Will be rescheduled
}


class JobStatusPoller:
    """Daemon thread that polls Slurm for job status updates."""

    def __init__(self, slurm: SlurmService, interval: int = 30):
        self.slurm = slurm
        self.interval = interval
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
        """Trigger an immediate status check (called by refresh endpoint)."""
        self._do_poll()

    def _poll_loop(self):
        while not self._stop_event.is_set():
            try:
                self._do_poll()
            except Exception as e:
                print(f"[Poller] Error: {e}")
            self._stop_event.wait(self.interval)

    def _do_poll(self):
        db = get_session()
        try:
            # Get all active jobs with cluster IDs
            active_jobs = (
                db.query(AnalysisJob)
                .filter(
                    AnalysisJob.status.in_(["queued", "running"]),
                    AnalysisJob.cluster_job_id.isnot(None),
                )
                .all()
            )

            if not active_jobs:
                return

            cluster_ids = [j.cluster_job_id for j in active_jobs]
            statuses = self.slurm.get_batch_status(cluster_ids)

            updated = 0
            for job in active_jobs:
                info = statuses.get(job.cluster_job_id)
                if not info:
                    continue

                slurm_state = info["state"].split()[0]  # e.g. "CANCELLED by 12345" → "CANCELLED"
                new_status = _STATE_MAP.get(slurm_state)
                if not new_status or new_status == job.status:
                    continue

                job.status = new_status

                if new_status == "running" and not job.started_at and info.get("start"):
                    try:
                        job.started_at = datetime.fromisoformat(info["start"])
                    except (ValueError, TypeError):
                        job.started_at = datetime.utcnow()

                if new_status in ("completed", "failed"):
                    if not job.completed_at:
                        if info.get("end"):
                            try:
                                job.completed_at = datetime.fromisoformat(info["end"])
                            except (ValueError, TypeError):
                                job.completed_at = datetime.utcnow()
                        else:
                            job.completed_at = datetime.utcnow()

                    if new_status == "failed":
                        exit_code = info.get("exit_code", "unknown")
                        job.error_message = f"Slurm state: {slurm_state}, exit: {exit_code}"

                updated += 1

            if updated:
                db.commit()
                print(f"[Poller] Updated {updated} job(s)")
            else:
                db.rollback()

        except Exception as e:
            db.rollback()
            raise
        finally:
            db.close()
