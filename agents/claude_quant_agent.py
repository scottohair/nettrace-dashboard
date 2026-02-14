#!/usr/bin/env python3
"""Claude Quant Agent — continuously runs Quant 100 validation cycles.

Responsibilities:
  - Build/refresh 100-item experiment plan
  - Run backtests and pipeline gating
  - Produce budget recommendations for promoted strategies
  - Persist status for dashboard/agent pool visibility
"""

import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import quant_100_runner as q100
try:
    import claude_staging as claude_staging
except Exception:
    claude_staging = None
try:
    import claude_duplex as claude_duplex
except Exception:
    claude_duplex = None

BASE_DIR = Path(__file__).parent
STATUS_FILE = BASE_DIR / "quant_100_agent_status.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [claude_quant_agent] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE_DIR / "claude_quant_agent.log")),
    ],
)
logger = logging.getLogger("claude_quant_agent")

INTERVAL_SECONDS = int(os.environ.get("QUANT100_INTERVAL_SECONDS", "14400"))  # 4h
BACKTEST_HOURS = int(os.environ.get("QUANT100_BACKTEST_HOURS", "72"))
GRANULARITY = os.environ.get("QUANT100_GRANULARITY", "5min")
CLAUDE_TEAM_COLLAB_ENABLED = os.environ.get("CLAUDE_TEAM_COLLAB_ENABLED", "1").lower() not in ("0", "false", "no")
CLAUDE_TEAM_STATE_FILE = BASE_DIR / "claude_team_state.json"
TASK_ID_RE = re.compile(r"\b(AF-\d{3}|WIN-\d{4})\b", re.IGNORECASE)
CLAUDE_TEAM_TOPICS = {
    "realized_pnl": ("realized", "close", "pnl", "profit"),
    "budget": ("budget", "escalat", "de_escalat", "allocation"),
    "risk": ("risk", "drawdown", "loss", "hardstop", "no-go"),
    "execution": ("latency", "dns", "fill", "exit", "execution"),
    "quant": ("quant", "strategy", "pipeline", "warm", "hot"),
}


class ClaudeQuantAgent:
    def __init__(self):
        self.running = True
        self.cycles = 0
        self.last_to_claude_id = 0
        self._load_state()

    def _load_state(self):
        try:
            if STATUS_FILE.exists():
                s = json.loads(STATUS_FILE.read_text())
                self.last_to_claude_id = int(s.get("last_to_claude_id", 0) or 0)
        except Exception:
            self.last_to_claude_id = 0

    def _write_status(self, state, summary=None, ingest=None, consumed=None, decision_trace=None, error=None):
        payload = {
            "state": state,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "cycle_count": self.cycles,
            "interval_seconds": INTERVAL_SECONDS,
            "backtest_hours": BACKTEST_HOURS,
            "granularity": GRANULARITY,
            "summary": summary or {},
            "ingest": ingest or {},
            "consumed_directives": consumed or {},
            "decision_trace": decision_trace or {},
            "last_to_claude_id": self.last_to_claude_id,
            "error": str(error) if error else "",
        }
        try:
            STATUS_FILE.write_text(json.dumps(payload, indent=2))
        except Exception as e:
            logger.warning("Could not write status file: %s", e)

    @staticmethod
    def _extract_pairs(text):
        if not text:
            return []
        return re.findall(r"\b[A-Z]{2,6}-USD\b", str(text).upper())

    @staticmethod
    def _extract_task_ids(text):
        if not text:
            return []
        return [str(x).upper() for x in TASK_ID_RE.findall(str(text))]

    @staticmethod
    def _normalize_priority_pairs(*pair_groups, limit=10):
        ordered = []
        for group in pair_groups:
            for p in group or []:
                p = str(p).upper().strip()
                if not re.fullmatch(r"[A-Z]{2,6}-USD", p):
                    continue
                if p not in ordered:
                    ordered.append(p)
                if len(ordered) >= limit:
                    return ordered
        return ordered[:limit]

    @staticmethod
    def _normalize_task_ids(*task_groups, limit=40):
        ordered = []
        for group in task_groups:
            for t in group or []:
                task_id = str(t).upper().strip()
                if not TASK_ID_RE.fullmatch(task_id):
                    continue
                if task_id not in ordered:
                    ordered.append(task_id)
                if len(ordered) >= limit:
                    return ordered
        return ordered[:limit]

    @staticmethod
    def _directive_digest(msgs):
        digest = {
            "total": len(msgs),
            "from_codex": 0,
            "high_priority": 0,
            "target_role_counts": {"opus": 0, "sonnet": 0},
            "topic_hits": {k: 0 for k in CLAUDE_TEAM_TOPICS.keys()},
        }
        for m in msgs:
            source = str(m.get("source", "")).lower()
            if source == "codex":
                digest["from_codex"] += 1
            if str(m.get("priority", "")).lower() == "high":
                digest["high_priority"] += 1
            meta = m.get("meta", {}) if isinstance(m.get("meta"), dict) else {}
            role = str(meta.get("target_role", "")).lower().strip()
            if role in digest["target_role_counts"]:
                digest["target_role_counts"][role] += 1
            text = str(m.get("message", "")).lower()
            for topic, needles in CLAUDE_TEAM_TOPICS.items():
                if any(n in text for n in needles):
                    digest["topic_hits"][topic] += 1
        return digest

    def _write_team_state(self, summary, priority_pairs, digest):
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "cycle": self.cycles,
            "priority_pairs": priority_pairs,
            "summary": {
                "total": int(summary.get("total", 0) or 0),
                "promoted": int(summary.get("promoted_warm", 0) or 0),
                "rejected": int(summary.get("rejected_cold", 0) or 0),
                "no_data": int(summary.get("no_data", 0) or 0),
            },
            "directive_digest": digest,
            "roles": {
                "opus": {
                    "focus": "research_hypotheses_and_risk_architecture",
                    "target": "maximize robust realized close-profit evidence",
                },
                "sonnet": {
                    "focus": "execution_and_pipeline_implementation",
                    "target": "ship deterministic exit and fill-quality improvements fast",
                },
            },
        }
        try:
            CLAUDE_TEAM_STATE_FILE.write_text(json.dumps(payload, indent=2))
        except Exception as e:
            logger.warning("Could not write claude team state: %s", e)

    def _emit_role_collaboration(self, summary, priority_pairs, digest):
        if claude_duplex is None or not CLAUDE_TEAM_COLLAB_ENABLED:
            return
        promoted = int(summary.get("promoted_warm", 0) or 0)
        rejected = int(summary.get("rejected_cold", 0) or 0)
        no_data = int(summary.get("no_data", 0) or 0)

        opus_message = (
            "Opus role update: refine strategy hypotheses toward realized-close-positive outcomes, "
            "tighten risk gates, and prefer high-confidence deterministic exits."
        )
        sonnet_message = (
            "Sonnet role update: implement execution fixes that increase filled SELL closes, "
            "improve fill quality, and reduce pending/open order drift."
        )
        consensus_message = (
            f"Opus+Sonnet consensus: cycle={self.cycles} promoted={promoted} rejected={rejected} no_data={no_data}. "
            "Joint plan: prioritize realized close-profit evidence before any budget escalation."
        )
        meta_common = {
            "cycle": self.cycles,
            "priority_pairs": priority_pairs,
            "directive_digest": digest,
            "summary": {
                "promoted": promoted,
                "rejected": rejected,
                "no_data": no_data,
            },
        }
        claude_duplex.send_from_claude(
            opus_message,
            msg_type="role_update",
            priority="high" if digest.get("high_priority", 0) > 0 else "normal",
            source="claude_opus",
            meta={**meta_common, "role": "opus"},
        )
        claude_duplex.send_from_claude(
            sonnet_message,
            msg_type="role_update",
            priority="high" if digest.get("high_priority", 0) > 0 else "normal",
            source="claude_sonnet",
            meta={**meta_common, "role": "sonnet"},
        )
        claude_duplex.send_from_claude(
            consensus_message,
            msg_type="team_consensus",
            priority="high" if promoted <= 0 else "normal",
            source="claude_team",
            meta={**meta_common, "roles": ["opus", "sonnet"]},
        )

    def _consume_duplex_directives(self):
        if claude_duplex is None:
            return [], [], [], []
        msgs = claude_duplex.read_to_claude(since_id=self.last_to_claude_id, limit=200)
        if not msgs:
            return [], [], [], []

        self.last_to_claude_id = max(int(m.get("id", 0)) for m in msgs)
        pairs = []
        trace_ids = []
        task_ids = []
        for m in msgs:
            meta = m.get("meta", {}) if isinstance(m.get("meta"), dict) else {}
            trace_id = str(m.get("trace_id") or meta.get("trace_id") or "").strip()
            if trace_id and trace_id not in trace_ids:
                trace_ids.append(trace_id)
            for p in meta.get("priority_pairs", []) or []:
                p = str(p).upper().strip()
                if p and p not in pairs:
                    pairs.append(p)
            for p in self._extract_pairs(m.get("message", "")):
                if p not in pairs:
                    pairs.append(p)
            inferred_task_ids = []
            inferred_task_ids.extend(self._extract_task_ids(m.get("message", "")))
            for key in ("task_id", "work_item_id"):
                val = str(meta.get(key, "")).upper().strip()
                if val:
                    inferred_task_ids.append(val)
            for key in ("task_ids", "work_item_ids"):
                vals = meta.get(key, [])
                if isinstance(vals, list):
                    inferred_task_ids.extend([str(v).upper().strip() for v in vals])
            task_ids = self._normalize_task_ids(task_ids, inferred_task_ids, limit=60)
        return msgs, pairs, trace_ids, task_ids

    def run_once(self):
        self.cycles += 1
        logger.info("Starting quant cycle %d", self.cycles)
        self._write_status("running")

        ingest = {}
        priority_pairs = []
        consumed_msgs = []
        directive_trace_ids = []
        directive_task_ids = []
        mcp_lesson = {}
        bundle_id = ""
        bundle_hash = ""
        bundle_sequence = 0
        if claude_staging is not None:
            try:
                bundle = claude_staging.get_latest_bundle()
                ingest = bundle.get("summary", {}) if isinstance(bundle, dict) else {}
                bundle_id = str(bundle.get("bundle_id", "") or bundle.get("metadata", {}).get("bundle_id", ""))
                bundle_hash = str(bundle.get("bundle_hash", "") or bundle.get("metadata", {}).get("bundle_hash", ""))
                bundle_sequence = int(bundle.get("bundle_sequence", 0) or bundle.get("metadata", {}).get("bundle_sequence", 0) or 0)
                priority_pairs = self._normalize_priority_pairs(
                    (ingest.get("hard_priority_pairs") or [])[:10],
                    (ingest.get("focus_pairs") or [])[:10],
                    limit=10,
                )
                mcp_lesson = bundle.get("mcp_curriculum", {}) if isinstance(bundle, dict) else {}
            except Exception:
                pass
        consumed_msgs, duplex_pairs, directive_trace_ids, directive_task_ids = self._consume_duplex_directives()
        priority_pairs = self._normalize_priority_pairs(priority_pairs, duplex_pairs, limit=10)
        directive_digest = self._directive_digest(consumed_msgs)

        experiments = q100.build_100_experiments(priority_pairs=priority_pairs)
        q100.save_json(q100.PLAN_FILE, {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(experiments),
            "experiments": experiments,
            "source": "claude_quant_agent",
            "priority_pairs": priority_pairs,
        })

        results = q100.run_experiments(experiments, hours=BACKTEST_HOURS, granularity=GRANULARITY)
        summary = q100.summarize(results)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": summary,
            "results": results,
            "source": "claude_quant_agent",
            "ingest": ingest,
            "priority_pairs": priority_pairs,
        }
        q100.save_json(q100.RESULTS_FILE, payload)
        consumed = {
            "message_count": len(consumed_msgs),
            "priority_pairs": priority_pairs,
            "mcp_lesson_title": mcp_lesson.get("title", ""),
            "directive_digest": directive_digest,
            "directive_trace_ids": directive_trace_ids,
            "directive_task_ids": directive_task_ids,
            "bundle_id": bundle_id,
            "bundle_hash": bundle_hash,
            "bundle_sequence": bundle_sequence,
        }
        decision_trace = {
            "bundle_id": bundle_id,
            "bundle_hash": bundle_hash,
            "bundle_sequence": bundle_sequence,
            "directive_trace_ids": directive_trace_ids,
            "directive_task_ids": directive_task_ids,
            "consumed_message_ids": [int(m.get("id", 0)) for m in consumed_msgs],
        }
        self._write_status("idle", summary=summary, ingest=ingest, consumed=consumed, decision_trace=decision_trace)
        self._write_team_state(summary, priority_pairs, directive_digest)

        if claude_duplex is not None:
            if consumed_msgs:
                claude_duplex.send_from_claude(
                    f"Acknowledged {len(consumed_msgs)} directive(s); applied {len(priority_pairs)} priority pairs.",
                    msg_type="ack",
                    priority="high",
                    source="claude_quant",
                    meta={
                        "reply_to_id": self.last_to_claude_id,
                        "priority_pairs": priority_pairs,
                        "directive_trace_ids": directive_trace_ids,
                        "task_ids": directive_task_ids,
                        "ingest_bundle_id": bundle_id,
                        "ingest_bundle_sequence": bundle_sequence,
                    },
                )
            if directive_task_ids:
                claude_duplex.send_from_claude(
                    f"Acknowledged work items: {', '.join(directive_task_ids[:12])}",
                    msg_type="work_ack",
                    priority="high",
                    source="claude_quant",
                    meta={
                        "reply_to_id": self.last_to_claude_id,
                        "task_ids": directive_task_ids,
                        "work_item_ids": directive_task_ids,
                        "directive_trace_ids": directive_trace_ids,
                        "ingest_bundle_id": bundle_id,
                        "ingest_bundle_sequence": bundle_sequence,
                    },
                )
            claude_duplex.send_from_claude(
                f"Quant cycle {self.cycles} complete: promoted={summary.get('promoted_warm', 0)} no_data={summary.get('no_data', 0)}",
                msg_type="cycle_report",
                priority="normal",
                source="claude_quant",
                meta={
                    "cycle": self.cycles,
                    "priority_pairs": priority_pairs,
                    "total": summary.get("total", 0),
                    "promoted": summary.get("promoted_warm", 0),
                    "rejected": summary.get("rejected_cold", 0),
                    "no_data": summary.get("no_data", 0),
                    "ingest_bundle_id": bundle_id,
                    "ingest_bundle_sequence": bundle_sequence,
                },
            )
            if mcp_lesson:
                claude_duplex.send_from_claude(
                    f"MCP lesson loaded: {mcp_lesson.get('title', 'MCP Quick Curriculum')}",
                    msg_type="mcp_reflection",
                    priority="normal",
                    source="claude_quant",
                    meta={
                        "lesson_title": mcp_lesson.get("title", ""),
                        "protocol_flow": mcp_lesson.get("protocol_flow", [])[:4],
                        "ingest_bundle_id": bundle_id,
                    },
                )
            self._emit_role_collaboration(summary, priority_pairs, directive_digest)

        logger.info(
            "Cycle %d complete: total=%d promoted=%d rejected=%d no_data=%d priority_pairs=%s directives=%d",
            self.cycles,
            summary.get("total", 0),
            summary.get("promoted_warm", 0),
            summary.get("rejected_cold", 0),
            summary.get("no_data", 0),
            ",".join(priority_pairs) if priority_pairs else "none",
            len(consumed_msgs),
        )
        return summary

    def run_forever(self):
        def _stop(signum, _frame):
            logger.info("Received signal %d, stopping...", signum)
            self.running = False

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        self._write_status("starting")
        logger.info(
            "Claude Quant Agent started (interval=%ds, hours=%d, granularity=%s)",
            INTERVAL_SECONDS, BACKTEST_HOURS, GRANULARITY
        )

        while self.running:
            try:
                self.run_once()
            except Exception as e:
                logger.error("Cycle failed: %s", e, exc_info=True)
                self._write_status("error", error=e)

            if not self.running:
                break

            self._write_status("sleeping")
            for _ in range(INTERVAL_SECONDS):
                if not self.running:
                    break
                time.sleep(1)

        self._write_status("stopped")
        logger.info("Claude Quant Agent stopped")


def print_status():
    if not STATUS_FILE.exists():
        print("No status file yet.")
        return
    print(STATUS_FILE.read_text())


if __name__ == "__main__":
    agent = ClaudeQuantAgent()

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd in ("--once", "once"):
            agent.run_once()
        elif cmd in ("--status", "status"):
            print_status()
        else:
            print("Usage: claude_quant_agent.py [--once|--status]")
    else:
        agent.run_forever()
