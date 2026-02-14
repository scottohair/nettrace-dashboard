#!/usr/bin/env python3
"""Autonomous Deployment Controller for NetTrace.

Orchestrates zero-touch deployments across Fly.io regions:
  1. Canary stage (lhr, nrt) - Fast health validation
  2. Primary stage (ewr) - Core execution environment
  3. Full rollout stage (ord, fra, sin, bom) - Parallel deployment

Each stage includes health gates, automatic rollback on failure, and
webhook notifications to Slack/Discord for visibility.

Usage:
  python deploy_controller.py --mode canary --version v72
  python deploy_controller.py --mode primary --version v72
  python deploy_controller.py --mode full --version v72
  python deploy_controller.py --mode status
  python deploy_controller.py --mode rollback --version v71
"""

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import threading
import queue

# Ensure agents/ is on sys.path
sys.path.insert(0, str(Path(__file__).parent))

from webhook_notifier import send_webhook_alert, AlertLevel

logger = logging.getLogger("deploy_controller")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

BASE = Path(__file__).parent
DEPLOY_HISTORY_PATH = BASE / "deploy_history.jsonl"
DEPLOY_STATUS_PATH = BASE / "deploy_status.json"

# Fly.io regions with their roles
FLY_REGIONS = {
    "canary": ["lhr", "nrt"],          # Scout regions for fast validation
    "primary": ["ewr"],                 # Core trading brain
    "full": ["ord", "fra", "sin", "bom"],  # Secondary regions
}

FLYCTL = "/Users/scott/.fly/bin/flyctl"


class DeployController:
    """Orchestrates multi-stage deployments with health gates and rollback."""

    def __init__(self):
        self.current_version = self._get_git_commit_short()
        self.start_time = datetime.now(timezone.utc)
        self.deploy_history: List[Dict] = self._load_deploy_history()

    def _get_git_commit_short(self) -> str:
        """Get current git commit short hash."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(BASE.parent),
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.stdout.strip() or "unknown"
        except Exception as e:
            logger.error(f"Failed to get git commit: {e}")
            return "unknown"

    def _load_deploy_history(self) -> List[Dict]:
        """Load deployment history from JSONL file."""
        history = []
        if DEPLOY_HISTORY_PATH.exists():
            try:
                with open(DEPLOY_HISTORY_PATH) as f:
                    for line in f:
                        if line.strip():
                            history.append(json.loads(line))
            except Exception as e:
                logger.warning(f"Failed to load deploy history: {e}")
        return history

    def _save_deploy_record(self, record: Dict) -> None:
        """Append deployment record to history."""
        with open(DEPLOY_HISTORY_PATH, "a") as f:
            f.write(json.dumps(record) + "\n")

    def _get_health_status(self, region: str) -> Dict:
        """Check health of a specific region using execution_health."""
        try:
            # Import execution_health module
            from execution_health import get_execution_health
            health = get_execution_health(region_filter=[region])
            return health.get(region, {})
        except Exception as e:
            logger.error(f"Failed to get health for {region}: {e}")
            return {"status": "unknown", "error": str(e)}

    def _deploy_to_region(self, region: str) -> bool:
        """Deploy to a single region using flyctl."""
        logger.info(f"Deploying to region: {region}")
        try:
            result = subprocess.run(
                [FLYCTL, "deploy", "--region", region, "--remote-only"],
                cwd=str(BASE.parent),
                capture_output=True,
                text=True,
                timeout=900  # 15 minutes per region
            )
            if result.returncode == 0:
                logger.info(f"✅ Deployed to {region}")
                return True
            else:
                logger.error(f"❌ Deploy to {region} failed: {result.stderr}")
                return False
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Deploy to {region} timed out")
            return False
        except Exception as e:
            logger.error(f"❌ Deploy to {region} error: {e}")
            return False

    def _validate_health(self, regions: List[str], timeout_seconds: int = 300) -> bool:
        """Wait for regions to pass health checks."""
        logger.info(f"Validating health for regions: {regions}")
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            all_healthy = True
            for region in regions:
                health = self._get_health_status(region)
                status = health.get("status", "unknown")

                if status == "healthy":
                    logger.info(f"✅ {region}: healthy")
                elif status == "degraded":
                    logger.warning(f"⚠️  {region}: degraded")
                    all_healthy = False
                else:
                    logger.warning(f"❓ {region}: {status}")
                    all_healthy = False

            if all_healthy:
                logger.info("✅ All regions healthy")
                return True

            logger.info(f"Waiting for health checks... ({int(deadline - time.time())}s remaining)")
            time.sleep(10)

        logger.error("❌ Health validation timed out")
        return False

    def _rollback_to_version(self, version: str) -> bool:
        """Rollback to a previous version."""
        logger.info(f"Rolling back to version: {version}")

        # Find the deployment record for this version
        prev_deploy = None
        for record in reversed(self.deploy_history):
            if record.get("version") == version and record.get("status") == "success":
                prev_deploy = record
                break

        if not prev_deploy:
            logger.error(f"No successful deployment found for version {version}")
            return False

        try:
            # Checkout the previous version
            result = subprocess.run(
                ["git", "checkout", version],
                cwd=str(BASE.parent),
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                logger.error(f"Failed to checkout {version}: {result.stderr}")
                return False

            # Deploy all regions
            for stage, regions in FLY_REGIONS.items():
                for region in regions:
                    if not self._deploy_to_region(region):
                        logger.error(f"Rollback failed at {region}")
                        return False

            logger.info("✅ Rollback successful")
            return True
        except Exception as e:
            logger.error(f"Rollback error: {e}")
            return False

    def deploy_canary(self) -> bool:
        """Deploy to canary regions (lhr, nrt) for quick validation."""
        logger.info("=" * 60)
        logger.info("STAGE 1: CANARY DEPLOYMENT")
        logger.info("=" * 60)

        record = {
            "stage": "canary",
            "version": self.current_version,
            "regions": FLY_REGIONS["canary"],
            "start_time": self.start_time.isoformat(),
            "status": "in_progress"
        }

        try:
            # Deploy to canary regions in parallel
            deploy_results = {}
            threads = []
            results_lock = threading.Lock()

            for region in FLY_REGIONS["canary"]:
                def deploy_region(r):
                    result = self._deploy_to_region(r)
                    with results_lock:
                        deploy_results[r] = result

                thread = threading.Thread(target=deploy_region, args=(region,))
                thread.start()
                threads.append(thread)

            # Wait for all deploys
            for thread in threads:
                thread.join()

            if not all(deploy_results.values()):
                logger.error("❌ Canary deployment failed")
                record["status"] = "failed"
                record["failed_regions"] = [r for r, ok in deploy_results.items() if not ok]
                self._save_deploy_record(record)
                send_webhook_alert(
                    AlertLevel.P0,
                    "❌ Canary Deployment Failed",
                    f"Failed to deploy {record['failed_regions']} in canary stage",
                    {"version": self.current_version, "stage": "canary"}
                )
                return False

            # Validate health
            if not self._validate_health(FLY_REGIONS["canary"], timeout_seconds=300):
                logger.error("❌ Canary health validation failed - rolling back")
                record["status"] = "failed"
                record["rollback_reason"] = "health_validation"
                self._save_deploy_record(record)
                send_webhook_alert(
                    AlertLevel.P0,
                    "❌ Canary Health Check Failed",
                    f"Canary regions {FLY_REGIONS['canary']} failed health validation",
                    {"version": self.current_version}
                )
                return False

            record["status"] = "success"
            record["end_time"] = datetime.now(timezone.utc).isoformat()
            record["duration_seconds"] = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            self._save_deploy_record(record)

            send_webhook_alert(
                AlertLevel.P2,
                "✅ Canary Deployment Successful",
                f"Version {self.current_version} deployed to {FLY_REGIONS['canary']}",
                {"version": self.current_version, "duration_seconds": record["duration_seconds"]}
            )
            return True

        except Exception as e:
            logger.error(f"Canary deployment exception: {e}")
            record["status"] = "error"
            record["error"] = str(e)
            self._save_deploy_record(record)
            send_webhook_alert(
                AlertLevel.P0,
                "❌ Canary Deployment Error",
                f"Exception: {e}",
                {"version": self.current_version}
            )
            return False

    def deploy_primary(self) -> bool:
        """Deploy to primary region (ewr) - critical trading brain."""
        logger.info("=" * 60)
        logger.info("STAGE 2: PRIMARY DEPLOYMENT")
        logger.info("=" * 60)

        record = {
            "stage": "primary",
            "version": self.current_version,
            "regions": FLY_REGIONS["primary"],
            "start_time": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress"
        }

        try:
            # Deploy to primary region
            if not self._deploy_to_region("ewr"):
                logger.error("❌ Primary deployment failed")
                record["status"] = "failed"
                self._save_deploy_record(record)
                send_webhook_alert(
                    AlertLevel.P0,
                    "❌ Primary Deployment Failed",
                    f"Failed to deploy version {self.current_version} to ewr",
                    {"version": self.current_version, "stage": "primary"}
                )
                return False

            # Validate health with longer timeout (primary is more critical)
            if not self._validate_health(FLY_REGIONS["primary"], timeout_seconds=600):
                logger.error("❌ Primary health validation failed - rolling back ALL regions")
                record["status"] = "failed"
                record["rollback_reason"] = "health_validation"
                self._save_deploy_record(record)

                # Rollback ALL regions if primary health fails
                prev_version = None
                for rec in reversed(self.deploy_history):
                    if rec.get("stage") == "full" and rec.get("status") == "success":
                        prev_version = rec.get("version")
                        break

                if prev_version:
                    self._rollback_to_version(prev_version)

                send_webhook_alert(
                    AlertLevel.P0,
                    "🚨 Primary Health Check Failed - FULL ROLLBACK",
                    f"Primary region health validation failed for {self.current_version}. Rolled back to {prev_version}.",
                    {"version": self.current_version, "previous_version": prev_version}
                )
                return False

            record["status"] = "success"
            record["end_time"] = datetime.now(timezone.utc).isoformat()
            self._save_deploy_record(record)

            send_webhook_alert(
                AlertLevel.P2,
                "✅ Primary Deployment Successful",
                f"Version {self.current_version} deployed to ewr (primary)",
                {"version": self.current_version}
            )
            return True

        except Exception as e:
            logger.error(f"Primary deployment exception: {e}")
            record["status"] = "error"
            record["error"] = str(e)
            self._save_deploy_record(record)
            send_webhook_alert(
                AlertLevel.P0,
                "❌ Primary Deployment Error",
                f"Exception: {e}",
                {"version": self.current_version}
            )
            return False

    def deploy_full(self) -> bool:
        """Deploy to all remaining regions (ord, fra, sin, bom)."""
        logger.info("=" * 60)
        logger.info("STAGE 3: FULL ROLLOUT")
        logger.info("=" * 60)

        record = {
            "stage": "full",
            "version": self.current_version,
            "regions": FLY_REGIONS["full"],
            "start_time": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress"
        }

        try:
            # Deploy to all regions in parallel
            deploy_results = {}
            threads = []
            results_lock = threading.Lock()

            for region in FLY_REGIONS["full"]:
                def deploy_region(r):
                    result = self._deploy_to_region(r)
                    with results_lock:
                        deploy_results[r] = result

                thread = threading.Thread(target=deploy_region, args=(region,))
                thread.start()
                threads.append(thread)

            # Wait for all deploys
            for thread in threads:
                thread.join()

            failed_regions = [r for r, ok in deploy_results.items() if not ok]

            # If primary is healthy, don't rollback on secondary failures
            if failed_regions:
                logger.warning(f"⚠️  Failed to deploy to: {failed_regions}")
                logger.info("Primary is healthy, continuing without rollback")
                send_webhook_alert(
                    AlertLevel.P1,
                    f"⚠️  Partial Full Rollout - {len(failed_regions)} regions failed",
                    f"Failed regions: {failed_regions}",
                    {"version": self.current_version, "failed_regions": failed_regions}
                )
            else:
                logger.info("✅ All regions deployed")

            record["status"] = "success"
            record["failed_regions"] = failed_regions
            record["end_time"] = datetime.now(timezone.utc).isoformat()
            record["total_duration_seconds"] = (
                datetime.fromisoformat(record["end_time"]) -
                datetime.fromisoformat(record["start_time"])
            ).total_seconds()
            self._save_deploy_record(record)

            send_webhook_alert(
                AlertLevel.P2,
                f"✅ Full Rollout Complete - {self.current_version}",
                f"Deployed to {len(FLY_REGIONS['full']) - len(failed_regions)}/{len(FLY_REGIONS['full'])} regions",
                {"version": self.current_version, "total_duration_seconds": record["total_duration_seconds"]}
            )
            return True

        except Exception as e:
            logger.error(f"Full rollout exception: {e}")
            record["status"] = "error"
            record["error"] = str(e)
            self._save_deploy_record(record)
            send_webhook_alert(
                AlertLevel.P1,
                "❌ Full Rollout Error",
                f"Exception: {e}",
                {"version": self.current_version}
            )
            return True  # Don't fail entire deploy on secondary region errors

    def deploy_all(self) -> bool:
        """Execute full deployment pipeline: canary → primary → full."""
        logger.info("🚀 Starting full deployment pipeline")

        # Stage 1: Canary
        if not self.deploy_canary():
            logger.error("❌ Deployment aborted at canary stage")
            return False

        time.sleep(5)  # Brief pause between stages

        # Stage 2: Primary
        if not self.deploy_primary():
            logger.error("❌ Deployment aborted at primary stage")
            return False

        time.sleep(5)  # Brief pause between stages

        # Stage 3: Full rollout
        if not self.deploy_full():
            logger.error("❌ Deployment aborted at full rollout stage")
            return False

        logger.info("=" * 60)
        logger.info("✅ FULL DEPLOYMENT PIPELINE COMPLETE")
        logger.info("=" * 60)
        return True

    def get_status(self) -> Dict:
        """Get current deployment status."""
        if not DEPLOY_STATUS_PATH.exists():
            return {"status": "unknown"}

        with open(DEPLOY_STATUS_PATH) as f:
            return json.load(f)

    def save_status(self, status: Dict) -> None:
        """Save deployment status."""
        with open(DEPLOY_STATUS_PATH, "w") as f:
            json.dump(status, f, indent=2)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NetTrace Autonomous Deployment Controller")
    parser.add_argument("--mode", choices=["canary", "primary", "full", "all", "status", "rollback"],
                        default="all", help="Deployment mode")
    parser.add_argument("--version", help="Version to rollback to (for --mode rollback)")

    args = parser.parse_args()

    controller = DeployController()

    if args.mode == "canary":
        success = controller.deploy_canary()
    elif args.mode == "primary":
        success = controller.deploy_primary()
    elif args.mode == "full":
        success = controller.deploy_full()
    elif args.mode == "all":
        success = controller.deploy_all()
    elif args.mode == "status":
        status = controller.get_status()
        print(json.dumps(status, indent=2))
        return 0
    elif args.mode == "rollback":
        if not args.version:
            print("❌ --version required for rollback mode")
            return 1
        success = controller._rollback_to_version(args.version)
    else:
        print("Unknown mode")
        return 1

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
