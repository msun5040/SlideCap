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
from sqlalchemy.orm import joinedload

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

    def ping(self, timeout: int = 5) -> bool:
        """
        Actively verify the SSH connection is alive by running 'echo ping'.
        Unlike run_command, this does NOT attempt to reconnect if the transport
        is dead — it simply returns False. Intended for health checks.
        """
        with self._lock:
            if self._client is None:
                return False
            transport = self._client.get_transport()
            if transport is None or not transport.is_active():
                return False
            # Transport reports active — verify with a real command
            try:
                stdin, stdout, stderr = self._client.exec_command("echo ping", timeout=timeout)
                stdout.channel.recv_exit_status()
                return True
            except Exception:
                return False

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
        # No -z: SVS/NDPI are already compressed; compression wastes CPU with no size benefit
        if shutil.which("sshpass") and shutil.which("rsync"):
            try:
                ssh_opts = (
                    f"ssh -p {self._port}"
                    " -o StrictHostKeyChecking=no"
                    " -o ServerAliveInterval=30"
                    " -o ServerAliveCountMax=6"
                )
                cmd = [
                    "sshpass", "-p", self._password,
                    "rsync", "-avP", "--no-perms", "--timeout=120",
                    "-e", ssh_opts,
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
        job_id: int,
        remote_wsi_dir: str,
        remote_output_dir: str,
        gpu_index: int,
        parameters: Optional[dict] = None,
    ) -> str:
        """
        Start a batch analysis job in a tmux session covering all slides in remote_wsi_dir.
        Returns the tmux session name.
        """
        import shlex as _shlex
        session_name = f"slidecap_{analysis.name.lower().replace(' ', '_')}_{job_id}"

        params = parameters or {}
        template_vars = {
            "wsi_dir": remote_wsi_dir,
            "outdir": remote_output_dir,
            "gpu": str(gpu_index),  # Physical GPU index — also exported via CUDA_VISIBLE_DEVICES
            "batch_size": str(params.get("batch_size", 4)),
            "model_path": params.get("model_path", ""),
        }

        if analysis.command_template:
            command = analysis.command_template.format(**template_vars)
        elif analysis.script_path:
            command = f"{analysis.script_path} {remote_wsi_dir} {remote_output_dir}"
        else:
            raise RuntimeError("Analysis has no command_template or script_path defined")

        parts = []
        if analysis.working_directory:
            parts.append(f"cd {analysis.working_directory}")
        if analysis.env_setup:
            parts.append(analysis.env_setup)

        # Clean any stale output from a previous run, then create fresh dir
        parts.append(f"rm -rf {_shlex.quote(remote_output_dir)}")
        parts.append(f"mkdir -p {_shlex.quote(remote_output_dir)}")
        parts.append(f"{command} 2>&1 | tee {_shlex.quote(remote_output_dir)}/run.log")

        full_command = " && ".join(parts)

        # After analysis (success or failure): delete the entire batch WSI dir
        full_command += (
            f"; rm -rf {_shlex.quote(remote_wsi_dir)}"
            f"; true"
        )

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
        self._poll_lock = threading.Lock()  # Prevent concurrent _do_poll() calls

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
        """Trigger an immediate status check (no-op if a poll is already running)."""
        if self._poll_lock.locked():
            print("[Poller] poll_now skipped — poll already in progress")
            return
        with self._poll_lock:
            self._do_poll()

    def _poll_loop(self):
        while not self._stop_event.is_set():
            try:
                if self.cluster.is_connected:
                    with self._poll_lock:
                        self._do_poll()
            except Exception as e:
                print(f"[Poller] Error: {e}")
            self._stop_event.wait(self.interval)

    def _do_poll(self):
        if not self.cluster.is_connected:
            return

        db = get_session()
        try:
            updated = 0
            affected_job_ids = set()

            # --- Part 1: Poll known tmux sessions ---
            active_slides = (
                db.query(JobSlide)
                .filter(
                    JobSlide.status.in_(["queued", "running", "transferring"]),
                    JobSlide.cluster_job_id.isnot(None),
                )
                .all()
            )

            # Group by tmux session — one SSH check per session
            session_groups: dict[str, list] = {}
            for js in active_slides:
                session_groups.setdefault(js.cluster_job_id, []).append(js)

            for session_name, group in session_groups.items():
                representative = group[0]
                try:
                    info = self.cluster.check_job_status(
                        session_name,
                        representative.remote_output_path or ""
                    )
                except Exception as e:
                    print(f"[Poller] Failed to check session {session_name}: {e}")
                    continue

                if info.get("log_tail"):
                    for js in group:
                        js.log_tail = info["log_tail"]

                if info["alive"]:
                    for js in group:
                        if js.status != "running":
                            js.status = "running"
                            if not js.started_at:
                                js.started_at = datetime.utcnow()
                            updated += 1
                            affected_job_ids.add(js.job_id)
                else:
                    log_tail = info.get("log_tail", "")
                    log_error = self._detect_log_error(log_tail)

                    if log_error:
                        for js in group:
                            js.status = "failed"
                            js.error_message = log_error
                            js.completed_at = datetime.utcnow()
                    else:
                        output_dir = representative.remote_output_path
                        has_output = False
                        if output_dir:
                            stdout, _, _ = self.cluster.run_command(
                                f"ls {output_dir}/ 2>/dev/null | head -20"
                            )
                            has_output = bool(stdout.strip())

                        if has_output:
                            for js in group:
                                js.status = "completed"
                                js.completed_at = datetime.utcnow()
                            print(f"[Poller] Session {session_name}: cluster job done, awaiting manual transfer")
                        else:
                            for js in group:
                                js.status = "failed"
                                js.error_message = "tmux session ended without output files"
                                js.completed_at = datetime.utcnow()

                    updated += len(group)
                    for js in group:
                        affected_job_ids.add(js.job_id)

            # --- Part 2: Recover orphaned slides (stuck 'transferring', no cluster_job_id) ---
            # This happens when start_job() succeeded but the DB commit marking them 'running' failed.
            orphaned = (
                db.query(JobSlide)
                .filter(
                    JobSlide.status == "transferring",
                    JobSlide.cluster_job_id.is_(None),
                    JobSlide.remote_output_path.isnot(None),
                )
                .all()
            )
            for js in orphaned:
                try:
                    stdout, _, _ = self.cluster.run_command(
                        f"ls {js.remote_output_path}/ 2>/dev/null | head -5"
                    )
                    if stdout.strip():
                        js.status = "completed"
                        js.completed_at = datetime.utcnow()
                        updated += 1
                        affected_job_ids.add(js.job_id)
                        print(f"[Poller] Recovered orphaned slide {js.id}: output found at {js.remote_output_path}")
                except Exception as e:
                    print(f"[Poller] Could not check orphaned slide {js.id}: {e}")

            # Recompute parent job statuses
            if affected_job_ids:
                for job_id in affected_job_ids:
                    job = db.query(AnalysisJob).filter_by(id=job_id).first()
                    if job:
                        self._recompute_job_status(job)

            # --- Part 3: Reconcile stale job statuses ---
            # Jobs can end up with a status that doesn't match their slides if a
            # slide was corrected after the job was last recomputed (e.g. NFS error
            # marked slide failed, output check then flipped it to completed, but the
            # job status was never updated again).
            stale_jobs = (
                db.query(AnalysisJob)
                .options(joinedload(AnalysisJob.slides))
                .filter(AnalysisJob.status.in_(["failed", "running"]))
                .all()
            )
            for job in stale_jobs:
                if not job.slides:
                    continue
                statuses = [js.status for js in job.slides]
                if any(s in ("running", "transferring", "pending") for s in statuses):
                    continue  # still genuinely active, skip
                expected = None
                if all(s == "completed" for s in statuses):
                    expected = "completed"
                elif all(s in ("completed", "failed") for s in statuses):
                    expected = "failed" if any(s == "failed" for s in statuses) else "completed"
                if expected and job.status != expected:
                    job.status = expected
                    if not job.completed_at:
                        job.completed_at = datetime.utcnow()
                    affected_job_ids.add(job.id)
                    updated += 1

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

    def _transfer_and_distribute(self, group: list, db) -> None:
        """Rsync the shared batch output dir from cluster, distribute files per slide
        to the network drive, then clean up staging and cluster output."""
        import shlex
        if not group:
            return

        representative = group[0]
        remote_output_dir = representative.remote_output_path
        job = representative.job
        analysis_name = job.model_name if job else "unknown"

        if not remote_output_dir or not self.analyses_path:
            for js in group:
                js.status = "failed"
                js.error_message = "No remote output path or analyses_path configured"
                js.completed_at = datetime.utcnow()
            return

        # Rsync entire batch output dir to a staging area
        staging_dir = self.analyses_path / f"_staging_{job.id if job else 'unknown'}"
        staging_dir.mkdir(parents=True, exist_ok=True)
        remote_path = remote_output_dir.rstrip("/") + "/"

        success = False
        if shutil.which("sshpass") and shutil.which("rsync"):
            try:
                cmd = [
                    "sshpass", "-p", self.cluster._password,
                    "rsync", "-av", "--no-perms",
                    "-e", f"ssh -p {self.cluster._port} -o StrictHostKeyChecking=no",
                    f"{self.cluster._username}@{self.cluster._host}:{remote_path}",
                    str(staging_dir) + "/",
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
                success = result.returncode == 0
                if not success:
                    print(f"[Transfer] rsync failed for job {job.id}: rc={result.returncode} {result.stderr[:200]}")
            except Exception as e:
                print(f"[Transfer] rsync error for job {job.id}: {e}")

        if not success:
            try:
                client = self.cluster._get_client()
                sftp = client.open_sftp()
                try:
                    self._sftp_get_recursive(sftp, remote_output_dir.rstrip("/"), staging_dir)
                    success = True
                finally:
                    sftp.close()
            except Exception as e:
                print(f"[Transfer] SFTP error for job {job.id}: {e}")

        if not success:
            # Transfer failed — check if local output already exists from a previous
            # successful transfer (e.g. remote dir was already cleaned up).
            all_have_local = all(
                js.slide and any(
                    (self.analyses_path / js.slide.slide_hash / analysis_name).iterdir()
                ) if js.slide and (self.analyses_path / js.slide.slide_hash / analysis_name).exists() else False
                for js in group
            )
            if all_have_local:
                print(f"[Transfer] Job {job.id if job else '?'}: remote gone but local output exists — marking completed")
                for js in group:
                    if js.slide:
                        js.status = "completed"
                        js.local_output_path = str(self.analyses_path / js.slide.slide_hash / analysis_name)
                        js.completed_at = datetime.utcnow()
                shutil.rmtree(staging_dir, ignore_errors=True)
                return
            for js in group:
                js.status = "failed"
                js.error_message = "Failed to transfer output from cluster"
                js.completed_at = datetime.utcnow()
            shutil.rmtree(staging_dir, ignore_errors=True)
            return

        # Parse progress.log for per-slide success/failure
        slide_results: dict[str, bool] = {}  # filename_stem → success
        progress_log = staging_dir / "progress.log"
        if progress_log.exists():
            for line in progress_log.read_text(errors="replace").splitlines():
                line = line.strip()
                if line.startswith("[SUCCESS] "):
                    stem = Path(line[len("[SUCCESS] "):]).stem
                    slide_results[stem] = True
                elif line.startswith("[FAILED] "):
                    stem = Path(line[len("[FAILED] "):]).stem
                    slide_results[stem] = False

        # Distribute output files per slide
        for js in group:
            slide = js.slide
            if not slide:
                js.status = "failed"
                js.error_message = "Slide record not found"
                js.completed_at = datetime.utcnow()
                continue

            if not js.filename:
                # Legacy single-slide job: move all non-log files to per-slide dir
                local_dir = self.analyses_path / slide.slide_hash / analysis_name
                local_dir.mkdir(parents=True, exist_ok=True)
                for f in staging_dir.iterdir():
                    if f.name not in ("run.log", "progress.log") and f.is_file():
                        shutil.move(str(f), str(local_dir / f.name))
                js.status = "completed"
                js.local_output_path = str(local_dir)
                js.completed_at = datetime.utcnow()
                continue

            stem = Path(js.filename).stem
            local_dir = self.analyses_path / slide.slide_hash / analysis_name
            local_dir.mkdir(parents=True, exist_ok=True)

            # Recursively find files whose name starts with this stem anywhere in staging_dir.
            # This handles both flat output (CellViT: staging/{stem}_cells.pt) and
            # deeply nested output (UNI: staging/20x_256px/features/{stem}.h5).
            matched_files = [
                p for p in staging_dir.rglob("*")
                if p.is_file()
                and p.name.startswith(stem)
                and p.name not in ("run.log", "progress.log")
            ]
            # Top-level directories named with this stem (models that create per-slide subdirs)
            matched_top_dirs = [
                p for p in staging_dir.iterdir()
                if p.is_dir() and p.name.startswith(stem)
            ]

            # Move files preserving relative directory structure
            for f in matched_files:
                rel = f.relative_to(staging_dir)
                dest = local_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(f), str(dest))

            # Move top-level stem-named subdirs
            for d in matched_top_dirs:
                dest = local_dir / d.name
                if dest.exists():
                    shutil.rmtree(dest)
                shutil.move(str(d), str(dest))

            found_output = bool(matched_files or matched_top_dirs)

            if stem in slide_results:
                if slide_results[stem]:
                    js.status = "completed"
                    js.local_output_path = str(local_dir)
                else:
                    js.status = "failed"
                    js.error_message = f"Analysis script reported failure for {js.filename}"
            elif found_output:
                js.status = "completed"
                js.local_output_path = str(local_dir)
            else:
                js.status = "failed"
                js.error_message = f"No output files found for {js.filename}"

            js.completed_at = datetime.utcnow()

        # Parse full run.log to extract per-slide log sections (for cell stats etc.)
        run_log_src = staging_dir / "run.log"
        slide_log_sections: dict[str, str] = {}  # stem → log section for that slide
        if run_log_src.exists():
            full_log = run_log_src.read_text(errors="replace")
            # Split log on "Processing WSI: {filename}" markers
            import re as _re
            wsi_pattern = _re.compile(r"Processing WSI:\s+(\S+)")
            sections: list[tuple[str, int]] = []  # (stem, start_index)
            for m in wsi_pattern.finditer(full_log):
                stem = Path(m.group(1)).stem
                sections.append((stem, m.start()))
            for i, (stem, start) in enumerate(sections):
                end = sections[i + 1][1] if i + 1 < len(sections) else len(full_log)
                slide_log_sections[stem] = full_log[start:end]

        # Copy shared logs (run.log, progress.log) to each completed slide's dir
        # and update log_tail with the slide-specific section
        for log_name in ("run.log", "progress.log"):
            log_src = staging_dir / log_name
            if log_src.exists():
                for js in group:
                    if js.local_output_path:
                        dest = Path(js.local_output_path) / log_name
                        try:
                            shutil.copy2(str(log_src), str(dest))
                        except Exception:
                            pass

        # Set per-slide log_tail and compute+cache cell_stats
        import ast as _ast
        import json as _json
        for js in group:
            if js.filename:
                stem = Path(js.filename).stem
                section = slide_log_sections.get(stem, "")
                if section:
                    js.log_tail = section[-4000:]  # last 4KB of slide's section
                    # Parse and cache cell stats now so GET /jobs/{id} never reads disk
                    stats = self._extract_cell_stats_from_text(section)
                    if stats:
                        js.cell_stats = _json.dumps(stats)

        # Cleanup: remove staging dir and cluster output dir
        shutil.rmtree(staging_dir, ignore_errors=True)
        self._cleanup_cluster_files_batch(remote_output_dir)
        print(f"[Transfer] Job {job.id if job else '?'}: distributed to {len(group)} slides")

    def _cleanup_cluster_files_batch(self, remote_output_dir: str) -> None:
        """Remove the batch output dir from the cluster after successful transfer."""
        import shlex
        remote_out = remote_output_dir.rstrip("/")
        _, _, rc = self.cluster.run_command(f"rm -rf {shlex.quote(remote_out)}")
        print(f"[Cleanup] rm -rf {remote_out}: rc={rc}")

    def _transfer_results(self, js: JobSlide) -> Optional[str]:
        """Rsync results from cluster back to network drive for a completed slide,
        then clean up both the output dir and the WSI file from the cluster."""
        if not self.analyses_path:
            print(f"[Transfer] No analyses_path configured")
            return None
        if not js.remote_output_path:
            print(f"[Transfer] JobSlide {js.id} has no remote_output_path")
            return None

        slide = js.slide
        if not slide:
            print(f"[Transfer] JobSlide {js.id} has no slide relationship loaded")
            return None

        if not self.cluster.is_connected:
            print(f"[Transfer] Cluster not connected")
            return None

        analysis_name = js.job.model_name if js.job else "unknown"
        local_dir = self.analyses_path / slide.slide_hash / analysis_name
        local_dir.mkdir(parents=True, exist_ok=True)

        remote_path = js.remote_output_path.rstrip("/") + "/"
        remote_dir = js.remote_output_path.rstrip("/")
        print(f"[Transfer] Slide {slide.slide_hash[:12]}: {remote_path} -> {local_dir}")

        success = False

        if shutil.which("sshpass") and shutil.which("rsync"):
            try:
                cmd = [
                    "sshpass", "-p", self.cluster._password,
                    "rsync", "-av", "--no-perms",
                    "-e", f"ssh -p {self.cluster._port} -o StrictHostKeyChecking=no",
                    f"{self.cluster._username}@{self.cluster._host}:{remote_path}",
                    str(local_dir) + "/",
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
                if result.returncode == 0:
                    print(f"[Transfer] rsync OK for {slide.slide_hash[:12]} -> {local_dir}")
                    success = True
                else:
                    print(f"[Transfer] rsync failed for {slide.slide_hash[:12]}: rc={result.returncode} stderr={result.stderr[:300]}")
            except Exception as e:
                print(f"[Transfer] rsync error for {slide.slide_hash[:12]}: {e}")

        if not success:
            # Fallback: paramiko SFTP (recursive)
            print(f"[Transfer] Using SFTP fallback for {slide.slide_hash[:12]}")
            try:
                client = self.cluster._get_client()
                sftp = client.open_sftp()
                try:
                    count = self._sftp_get_recursive(sftp, remote_dir, local_dir)
                    print(f"[Transfer] SFTP OK for {slide.slide_hash[:12]}: {count} files -> {local_dir}")
                    success = True
                finally:
                    sftp.close()
            except Exception as e:
                print(f"[Transfer] SFTP error for {slide.slide_hash[:12]}: {e}")

        if success:
            self._cleanup_cluster_files(js)
            return str(local_dir)
        return None

    def _sftp_get_recursive(self, sftp, remote_dir: str, local_dir: Path) -> int:
        """Recursively download a remote directory tree via SFTP. Returns file count."""
        import stat as stat_module
        local_dir.mkdir(parents=True, exist_ok=True)
        count = 0
        for entry in sftp.listdir_attr(remote_dir):
            remote_path = f"{remote_dir}/{entry.filename}"
            local_path = local_dir / entry.filename
            if stat_module.S_ISDIR(entry.st_mode):
                count += self._sftp_get_recursive(sftp, remote_path, local_path)
            else:
                print(f"[Transfer]   SFTP get: {entry.filename} ({entry.st_size / 1024:.0f} KB)")
                sftp.get(remote_path, str(local_path))
                count += 1
        return count

    def _cleanup_cluster_files(self, js: JobSlide) -> None:
        """Remove output dir from cluster after successful transfer (legacy single-slide path)."""
        if js.remote_output_path:
            self._cleanup_cluster_files_batch(js.remote_output_path)

    @staticmethod
    def _extract_cell_stats_from_text(text: str) -> Optional[dict]:
        """Scan log text (bottom-up) for the last dict-like line of numeric cell counts."""
        import ast as _ast
        for line in reversed(text.splitlines()):
            line = line.strip()
            if " - " in line:
                line = line.split(" - ", 2)[-1].strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    parsed = _ast.literal_eval(line)
                    if isinstance(parsed, dict) and all(isinstance(v, (int, float)) for v in parsed.values()):
                        return parsed
                except Exception:
                    continue
        return None

    @staticmethod
    def _detect_log_error(log_tail: str) -> Optional[str]:
        """Scan the tail of run.log for Python exceptions or known error patterns.
        Returns a short error string if an error is found, else None."""
        import re
        if not log_tail:
            return None
        lines = [l.rstrip() for l in log_tail.splitlines()]
        # Walk lines in reverse to find the last meaningful error
        for line in reversed(lines):
            stripped = line.strip()
            if not stripped:
                continue
            # CUDA / GPU OOM
            if "OutOfMemoryError" in stripped or "out of memory" in stripped.lower():
                return stripped[:300]
            # Benign NFS cleanup artifact — not a real failure
            if "Device or resource busy" in stripped and ".nfs" in stripped:
                continue
            # Any named Python exception at the end of a traceback
            if re.match(r"[A-Za-z][A-Za-z0-9_.]*Error[:\s]", stripped):
                return stripped[:300]
            if re.match(r"[A-Za-z][A-Za-z0-9_.]*Exception[:\s]", stripped):
                return stripped[:300]
        # Fallback: traceback present but error line not in tail
        if "Traceback (most recent call last)" in log_tail:
            return "Python exception (see run.log for details)"
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
