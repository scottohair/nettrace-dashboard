#!/usr/bin/env python3
"""Fly migration control plane for quant + clawdbot/OpenClaw instances.

Builds an execution-ready migration plan so Python trading tooling runs on Fly
regions instead of only local runtime.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

try:
    from fly_deployer import DEPLOYMENT_MANIFEST, get_region
except Exception:
    try:
        from agents.fly_deployer import DEPLOYMENT_MANIFEST, get_region  # type: ignore
    except Exception:
        DEPLOYMENT_MANIFEST = {}

        def get_region():
            return "local"

try:
    import claude_duplex
except Exception:
    try:
        from agents import claude_duplex  # type: ignore
    except Exception:
        claude_duplex = None

BASE = Path(__file__).parent
ROOT = BASE.parent
PLAN_PATH = BASE / "fly_migration_plan.json"
STATUS_PATH = BASE / "fly_migration_status.json"
ROADMAP_PATH = BASE / "fly_migration_roadmap.md"
DEPLOYMENT_OPTIMIZER_PLAN_PATH = BASE / "deployment_optimizer_plan.json"

DEFAULT_INTERVAL_SECONDS = int(os.environ.get("FLY_MIGRATION_INTERVAL_SECONDS", "600"))
PRIMARY_REGION = str(os.environ.get("PRIMARY_REGION", "ewr")).strip() or "ewr"
NETTRACE_APP_BASENAME = str(os.environ.get("FLY_NETTRACE_APP_BASENAME", "nettrace-dashboard")).strip() or "nettrace-dashboard"
CLAWDBOT_APP_PREFIX = str(os.environ.get("FLY_CLAWDBOT_APP_PREFIX", "clawdbot")).strip() or "clawdbot"
CLAWDBOT_ENABLE = os.environ.get("FLY_CLAWDBOT_ENABLE", "1").lower() not in {"0", "false", "no"}
NETTRACE_VOLUME_GB = int(os.environ.get("FLY_NETTRACE_VOLUME_GB", "5"))
CLAWDBOT_VOLUME_GB = int(os.environ.get("FLY_CLAWDBOT_VOLUME_GB", "3"))
NETTRACE_FLY_CONFIG_PATH = str(os.environ.get("FLY_NETTRACE_CONFIG_PATH", "fly.toml")).strip() or "fly.toml"
CLAWDBOT_FLY_CONFIG_PATH = str(os.environ.get("FLY_CLAWDBOT_CONFIG_PATH", "tools/openclaw/fly.toml")).strip() or "tools/openclaw/fly.toml"
ENABLE_CLAUDE_UPDATES = os.environ.get("FLY_MIGRATION_CLAUDE_UPDATES", "1").lower() not in {"0", "false", "no"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [fly_migration] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "fly_migration_controller.log")),
    ],
)
logger = logging.getLogger("fly_migration")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_json(path, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except Exception:
        return default


def _save_json(path, payload):
    Path(path).write_text(json.dumps(payload, indent=2))


def _region_order():
    optimizer = _load_json(DEPLOYMENT_OPTIMIZER_PLAN_PATH, {})
    ranking = optimizer.get("region_ranking", []) if isinstance(optimizer.get("region_ranking"), list) else []
    ranked = []
    for row in ranking:
        if not isinstance(row, dict):
            continue
        region = str(row.get("region", "")).strip()
        if region and region in DEPLOYMENT_MANIFEST and region not in ranked:
            ranked.append(region)
    for region in DEPLOYMENT_MANIFEST.keys():
        if region not in ranked:
            ranked.append(region)
    return ranked


def _render_roadmap(plan):
    summary = plan.get("summary", {}) if isinstance(plan.get("summary"), dict) else {}
    regions = plan.get("regions", []) if isinstance(plan.get("regions"), list) else []
    actions = plan.get("priority_actions", []) if isinstance(plan.get("priority_actions"), list) else []

    lines = [
        "# Fly Migration Roadmap",
        "",
        f"Updated: {plan.get('updated_at', '')}",
        "",
        "## Summary",
        f"- Runtime region: {summary.get('runtime_region', 'local')}",
        f"- Regions in rollout scope: {summary.get('region_count', 0)}",
        f"- Quant apps planned: {summary.get('nettrace_apps', 0)}",
        f"- Clawdbot apps planned: {summary.get('clawdbot_apps', 0)}",
        f"- Target mode: {summary.get('target_mode', 'fly_primary_remote')}",
        "",
        "## Priority Actions",
    ]
    for action in actions[:20]:
        lines.append(f"- {action}")

    lines += ["", "## Region Matrix"]
    for row in regions:
        if not isinstance(row, dict):
            continue
        lines.append(
            f"- {row.get('region')}: nettrace_app={row.get('nettrace_app')} "
            f"clawdbot_app={row.get('clawdbot_app', 'disabled')} "
            f"agents={','.join(row.get('agent_names', [])[:8])}"
        )
    return "\n".join(lines).strip() + "\n"


def _nettrace_commands(app_name, region):
    return [
        f"fly apps create {app_name} || true",
        f"fly volumes create {app_name.replace('-', '_')}_data --region {region} --size {max(1, NETTRACE_VOLUME_GB)} -a {app_name} || true",
        (
            f"fly secrets set -a {app_name} ENABLE_AGENTS=1 PRIMARY_REGION={PRIMARY_REGION} "
            f"FLY_REGION_TARGET={region}"
        ),
        f"fly deploy -a {app_name} -c {NETTRACE_FLY_CONFIG_PATH} --region {region}",
    ]


def _clawdbot_commands(app_name, region):
    return [
        f"fly apps create {app_name} || true",
        f"fly volumes create {app_name.replace('-', '_')}_data --region {region} --size {max(1, CLAWDBOT_VOLUME_GB)} -a {app_name} || true",
        (
            f"fly secrets set -a {app_name} OPENCLAW_STATE_DIR=/data "
            "OPENCLAW_GATEWAY_TOKEN=<set-token> NETTRACE_URL=https://nettrace-dashboard.fly.dev "
            "NETTRACE_API_KEY=<set-key>"
        ),
        f"fly deploy -a {app_name} -c {CLAWDBOT_FLY_CONFIG_PATH} --region {region}",
    ]


class FlyMigrationController:
    def __init__(self, interval_seconds=DEFAULT_INTERVAL_SECONDS):
        self.interval_seconds = max(60, int(interval_seconds))
        self.running = True
        self.cycle = 0

    def _build_plan(self):
        runtime_region = str(get_region() if callable(get_region) else "local")
        ordered_regions = _region_order()
        regions = []
        for region in ordered_regions:
            info = DEPLOYMENT_MANIFEST.get(region, {}) if isinstance(DEPLOYMENT_MANIFEST, dict) else {}
            agent_defs = info.get("agents", []) if isinstance(info.get("agents"), list) else []
            agent_names = [str(a.get("name")) for a in agent_defs if isinstance(a, dict) and a.get("name")]

            nettrace_app = NETTRACE_APP_BASENAME if region == PRIMARY_REGION else f"{NETTRACE_APP_BASENAME}-{region}"
            clawdbot_app = (f"{CLAWDBOT_APP_PREFIX}-{region}" if CLAWDBOT_ENABLE else "")

            row = {
                "region": region,
                "role": str(info.get("role", "")),
                "agent_names": agent_names,
                "agent_count": len(agent_names),
                "priority_agents": [str(a.get("name")) for a in agent_defs if isinstance(a, dict) and int(a.get("priority", 0) or 0) == 1],
                "exchanges": [str(x) for x in info.get("exchanges", [])] if isinstance(info.get("exchanges"), list) else [],
                "nettrace_app": nettrace_app,
                "nettrace_deploy_commands": _nettrace_commands(nettrace_app, region),
            }
            if CLAWDBOT_ENABLE:
                row["clawdbot_app"] = clawdbot_app
                row["clawdbot_deploy_commands"] = _clawdbot_commands(clawdbot_app, region)
            regions.append(row)

        priority_actions = [
            "Deploy/scale nettrace app per region and keep ENABLE_AGENTS=1 everywhere.",
            "Pin primary treasury + allocation authority in PRIMARY_REGION; run scout stacks in secondary regions.",
            "Deploy one clawdbot/OpenClaw instance per region for local tool/agent orchestration.",
            "Route quant agent control through API keys in Fly secrets, never through repository files.",
            "Cut local-only orchestrator loops after remote health + PnL close reconciliation is green.",
        ]

        return {
            "updated_at": _now_iso(),
            "summary": {
                "runtime_region": runtime_region,
                "region_count": len(regions),
                "nettrace_apps": len(regions),
                "clawdbot_apps": len(regions) if CLAWDBOT_ENABLE else 0,
                "target_mode": "fly_primary_remote",
                "primary_region": PRIMARY_REGION,
            },
            "priority_actions": priority_actions,
            "regions": regions,
        }

    def run_cycle(self):
        self.cycle += 1
        plan = self._build_plan()
        _save_json(PLAN_PATH, plan)
        ROADMAP_PATH.write_text(_render_roadmap(plan))
        status = {
            "updated_at": _now_iso(),
            "cycle": int(self.cycle),
            "runtime_region": plan.get("summary", {}).get("runtime_region", "local"),
            "region_count": int(plan.get("summary", {}).get("region_count", 0) or 0),
            "primary_region": str(plan.get("summary", {}).get("primary_region", PRIMARY_REGION)),
            "target_mode": str(plan.get("summary", {}).get("target_mode", "fly_primary_remote")),
            "plan_path": str(PLAN_PATH),
            "roadmap_path": str(ROADMAP_PATH),
        }
        _save_json(STATUS_PATH, status)

        if ENABLE_CLAUDE_UPDATES and claude_duplex is not None:
            try:
                claude_duplex.write_to_claude(
                    {
                        "cycle": int(self.cycle),
                        "runtime_region": status["runtime_region"],
                        "primary_region": status["primary_region"],
                        "region_count": status["region_count"],
                        "plan_path": str(PLAN_PATH),
                    },
                    msg_type="fly_migration_plan",
                    source="fly_migration_controller",
                    priority="high",
                )
            except Exception:
                pass

        logger.info(
            "cycle=%d regions=%d primary=%s runtime=%s",
            self.cycle,
            status["region_count"],
            status["primary_region"],
            status["runtime_region"],
        )
        return {"plan": plan, "status": status}

    def run(self):
        logger.info("starting fly migration controller loop interval=%ss", self.interval_seconds)
        while self.running:
            started = time.time()
            try:
                self.run_cycle()
            except Exception as e:
                logger.error("cycle failed: %s", e, exc_info=True)
            elapsed = time.time() - started
            sleep_for = max(5, self.interval_seconds - int(elapsed))
            time.sleep(sleep_for)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fly migration control plane")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS, help="loop interval seconds")
    args = parser.parse_args(argv)

    controller = FlyMigrationController(interval_seconds=max(60, int(args.interval)))
    if args.once:
        result = controller.run_cycle()
        print(json.dumps(result, indent=2))
        return 0
    controller.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
