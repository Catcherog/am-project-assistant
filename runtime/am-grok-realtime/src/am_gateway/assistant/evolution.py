"""Evidence-driven, bounded configuration evolution for the AM assistant.

This is an offline/background controller, not a business executor. It changes
only five host-allowlisted reply settings. The host supplies the proposer,
source-bound replay worker and frozen-suite loader. No callback is synthesized
when a real binding is absent, and no model-produced command is executed.

The proposer sees development cases only. The replay worker receives inputs,
not oracles; the independent quality evaluator keeps the expected results.
Suites, configurations and attempts are versioned and hashed. Activation is a
SQLite configuration pointer change in an explicitly enabled host scope. It is
not evidence of deployment, phone acceptance or training of model weights.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
import json
import math
import time
import uuid

from ..models import ContractError, dumps, key


CONFIG_KEYS = frozenset({"reply.layout", "reply.max_chars", "reply.model_id",
                         "reply.model_budget_seconds", "reply.cache_ttl_seconds"})
MODEL_KEYS = frozenset({"reply.model_id", "reply.model_budget_seconds"})
VERSION = "am-assistant-evolution/1"
POLICY_VERSION = "am-assistant-promotion-policy/1"
PROMOTION_POLICIES = frozenset({"absolute_release", "relative_quality"})


class ReplayDeferred(Exception):
    """Internal safe-boundary checkpoint, before any new call is submitted."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def _text(value, field):
    if not isinstance(value, str) or not value.strip() or len(value) > 4096:
        raise ContractError("evolution invalid " + field)
    return value


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


@dataclass(frozen=True)
class ReplayBudget:
    """Total candidate budget; failures and timeouts consume calls too.

    A host callback must enforce the supplied timeout on its own process or
    transport. The controller also rejects late results and stops further work;
    it does not claim that an arbitrary Python callback was physically killed.
    """

    max_calls: int = 300
    max_seconds: float = 600.0
    per_call_seconds: float = 15.0
    repetitions: int = 1

    def validate(self):
        if type(self.max_calls) is not int or not 1 <= self.max_calls <= 10000:
            raise ContractError("evolution invalid max_calls")
        if type(self.repetitions) is not int or not 1 <= self.repetitions <= 10:
            raise ContractError("evolution invalid repetitions")
        if not _finite(self.max_seconds) or not 0 < self.max_seconds <= 86400:
            raise ContractError("evolution invalid max_seconds")
        if not _finite(self.per_call_seconds) or not 0 < self.per_call_seconds <= self.max_seconds:
            raise ContractError("evolution invalid per_call_seconds")
        return self


class EvolutionEngine:
    """Small host-bound loop from ImprovementJournal proposals to local config.

    Callback contracts:
      suite_loader(ref) -> list[case] or {"cases": list[case]}
      proposer(*, proposal, observations, feedback, baseline_config,
               development_cases, allowed_config_keys, known_model_ids,
               timeout_seconds) -> {"patch": {...}, "rationale": str}
      replayer(*, case, config, context) -> quality-v2 trial record

    ``context`` is evaluation metadata, not an agent prompt. It includes the
    required attempt identity and remaining timeout. Actual source checking and
    evidence references belong to the host's independent replay verifier.
    """

    def __init__(self, store, owner_id, *, proposer=None, replayer=None,
                 suite_loader=None, known_model_ids=(), enabled_scopes=(),
                 allowed_config_keys=CONFIG_KEYS, host_source_freshness_seconds=60,
                 evaluation_layer="offline", minimum_latency_gain=0.05,
                 runtime_revision="unspecified", monotonic=time.monotonic,
                 promotion_policy="absolute_release", relative_quality_scopes=(),
                 should_defer=None, host_remaining_seconds=None):
        from .quality import LAYERS
        self.store = store
        self.owner_id = _text(owner_id, "owner_id")
        self.proposer, self.replayer, self.suite_loader = proposer, replayer, suite_loader
        self.known_model_ids = frozenset(_text(x, "model_id") for x in known_model_ids)
        self.enabled_scopes = frozenset(_text(x, "enabled_scope") for x in enabled_scopes)
        self.allowed_config_keys = frozenset(allowed_config_keys)
        if not self.allowed_config_keys.issubset(CONFIG_KEYS):
            raise ContractError("evolution configuration allowlist contains protected keys")
        if not _finite(host_source_freshness_seconds) or host_source_freshness_seconds <= 0:
            raise ContractError("evolution invalid source freshness bound")
        if evaluation_layer not in LAYERS:
            raise ContractError("evolution invalid evaluation layer")
        if not _finite(minimum_latency_gain) or not 0 <= minimum_latency_gain < 1:
            raise ContractError("evolution invalid minimum latency gain")
        self.source_freshness = float(host_source_freshness_seconds)
        self.layer, self.minimum_latency_gain = evaluation_layer, float(minimum_latency_gain)
        self.runtime_revision = _text(runtime_revision, "runtime_revision")
        if promotion_policy not in PROMOTION_POLICIES:
            raise ContractError("evolution invalid promotion policy")
        self.promotion_policy = promotion_policy
        self.relative_quality_scopes = frozenset(
            _text(x, "relative quality scope") for x in relative_quality_scopes)
        if promotion_policy == "relative_quality" and evaluation_layer == "offline":
            raise ContractError("relative quality requires actual model replay")
        self.monotonic = monotonic
        if should_defer is not None and not callable(should_defer):
            raise ContractError("evolution should_defer must be callable")
        if host_remaining_seconds is not None and not callable(host_remaining_seconds):
            raise ContractError("evolution host_remaining_seconds must be callable")
        self.should_defer = should_defer
        self.host_remaining_seconds = host_remaining_seconds
        statements = (
            """CREATE TABLE IF NOT EXISTS r7_evo_suites(
                owner_id TEXT NOT NULL,suite_ref TEXT NOT NULL,manifest_json TEXT NOT NULL,
                manifest_hash TEXT NOT NULL,created REAL NOT NULL,PRIMARY KEY(owner_id,suite_ref))""",
            """CREATE TABLE IF NOT EXISTS r7_evo_versions(
                owner_id TEXT NOT NULL,scope TEXT NOT NULL,version INTEGER NOT NULL,
                parent_version INTEGER NOT NULL,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,
                candidate_id TEXT NOT NULL,created REAL NOT NULL,PRIMARY KEY(owner_id,scope,version))""",
            """CREATE TABLE IF NOT EXISTS r7_evo_active(
                owner_id TEXT NOT NULL,scope TEXT NOT NULL,version INTEGER NOT NULL,
                PRIMARY KEY(owner_id,scope))""",
            """CREATE TABLE IF NOT EXISTS r7_evo_candidates(
                candidate_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,scope TEXT NOT NULL,
                payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,state TEXT NOT NULL,
                active_run_id TEXT NOT NULL DEFAULT '',created REAL NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS r7_evo_runs(
                run_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,candidate_id TEXT NOT NULL,
                state TEXT NOT NULL,started REAL NOT NULL,finished REAL,
                report_json TEXT NOT NULL DEFAULT '',report_hash TEXT NOT NULL DEFAULT '')""",
            """CREATE TABLE IF NOT EXISTS r7_evo_trials(
                run_id TEXT NOT NULL,split TEXT NOT NULL,variant TEXT NOT NULL,
                case_id TEXT NOT NULL,attempt_id TEXT NOT NULL,record_json TEXT NOT NULL,
                record_hash TEXT NOT NULL,created REAL NOT NULL,
                PRIMARY KEY(run_id,split,variant,case_id,attempt_id))""",
            """CREATE TABLE IF NOT EXISTS r7_evo_proposal_attempts(
                attempt_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,proposal_id TEXT NOT NULL,
                state TEXT NOT NULL,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,created REAL NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS r8_evo_replay_progress(
                run_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,candidate_id TEXT NOT NULL,
                calls_used INTEGER NOT NULL,active_seconds REAL NOT NULL,
                epoch INTEGER NOT NULL,state TEXT NOT NULL,updated REAL NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS r8_evo_attempts(
                run_id TEXT NOT NULL,split TEXT NOT NULL,variant TEXT NOT NULL,
                case_id TEXT NOT NULL,attempt_id TEXT NOT NULL,state TEXT NOT NULL,
                epoch INTEGER NOT NULL,started REAL NOT NULL,finished REAL,
                elapsed_seconds REAL,record_hash TEXT NOT NULL DEFAULT '',
                reserved_seconds REAL NOT NULL DEFAULT 0,
                PRIMARY KEY(run_id,split,variant,case_id,attempt_id))""",
        )
        with store.transaction() as db:
            for statement in statements:
                db.execute(statement)
            if 'reserved_seconds' not in {r['name'] for r in db.execute('PRAGMA table_info(r8_evo_attempts)')}:
                db.execute('ALTER TABLE r8_evo_attempts ADD COLUMN reserved_seconds REAL NOT NULL DEFAULT 0')

    def _policy_identity(self, scope):
        """Pin the gate and explicit host scope, separately from release targets.

        The relative gate permits a measured local configuration improvement.
        It cannot authorize a different scope, remove suite requirements or
        turn an unmet product latency target into a successful release.
        """
        if self.promotion_policy == "relative_quality":
            if scope not in self.relative_quality_scopes:
                raise ContractError("relative quality scope is not explicitly authorized")
            if self.layer == "offline":
                raise ContractError("relative quality requires actual model replay")
        return {"schema_version": POLICY_VERSION, "mode": self.promotion_policy,
                "scope": scope, "evaluation_layer": self.layer,
                "minimum_latency_gain": self.minimum_latency_gain}

    def _check_candidate_policy(self, candidate):
        current = self._policy_identity(candidate["scope"])
        recorded = candidate.get("promotion_policy")
        if recorded is None:
            # Pre-policy R7 evidence only ever used the absolute gate. It is
            # never reinterpreted under a newly enabled relative policy.
            recorded = {**current, "mode": "absolute_release",
                        "evaluation_layer": candidate["evaluation_layer"],
                        "minimum_latency_gain": candidate["required_minimum_latency_gain"]}
        if recorded != current:
            raise ContractError("evolution promotion policy changed; create a new candidate")
        return recorded

    def _validate_config(self, config, *, patch=False):
        if not isinstance(config, dict) or not config:
            raise ContractError("evolution configuration must be a nonempty object")
        if not set(config).issubset(self.allowed_config_keys):
            raise ContractError("evolution configuration contains protected or unknown keys")
        for name, value in config.items():
            if name == "reply.layout" and (not isinstance(value, str) or value not in {"compact", "paragraph"}):
                raise ContractError("evolution invalid reply layout")
            if name == "reply.max_chars" and (type(value) is not int or not 240 <= value <= 1200):
                raise ContractError("evolution invalid reply length")
            if name == "reply.model_id" and (not isinstance(value, str) or value not in self.known_model_ids):
                raise ContractError("evolution unknown model id")
            if name == "reply.model_budget_seconds" and (not _finite(value) or not .2 <= value <= 3):
                raise ContractError("evolution invalid model latency budget")
            if name == "reply.cache_ttl_seconds" and (
                    not _finite(value) or not 5 <= value <= min(60, self.source_freshness)):
                raise ContractError("evolution cache TTL exceeds source freshness")
        dumps(config)
        return deepcopy(config)

    @staticmethod
    def _decoded(row, *, payload="payload_json", digest="payload_hash"):
        if not row:
            raise ContractError("evolution object unavailable")
        value = json.loads(row[payload])
        if key(value) != row[digest]:
            raise ContractError("evolution immutable object integrity mismatch")
        return value

    def _version(self, scope, version, db=None):
        query = "SELECT * FROM r7_evo_versions WHERE owner_id=? AND scope=? AND version=?"
        args = (self.owner_id, scope, version)
        row = db.execute(query, args).fetchone() if db else self.store.one(query, args)
        value = self._decoded(row)
        self._validate_config(value["config"])
        return value

    def register_baseline(self, scope, config):
        scope, config = _text(scope, "scope"), self._validate_config(config)
        with self.store.transaction() as db:
            active = db.execute("SELECT version FROM r7_evo_active WHERE owner_id=? AND scope=?",
                                (self.owner_id, scope)).fetchone()
            if active:
                current = self._version(scope, active["version"], db)
                if current["config"] != config:
                    raise ContractError("evolution baseline version conflict")
                return current
            payload = {"scope": scope, "version": 1, "parent_version": 0,
                       "config": config, "candidate_id": "", "reason": "host_registered_baseline"}
            db.execute("INSERT INTO r7_evo_versions VALUES(?,?,?,?,?,?,?,?)",
                       (self.owner_id, scope, 1, 0, dumps(payload), key(payload), "", self.store.clock()))
            db.execute("INSERT INTO r7_evo_active VALUES(?,?,1)", (self.owner_id, scope))
        return payload

    def active_config(self, scope):
        row = self.store.one("SELECT version FROM r7_evo_active WHERE owner_id=? AND scope=?",
                             (self.owner_id, _text(scope, "scope")))
        return self._version(scope, row["version"]) if row else None

    @staticmethod
    def _cases(value):
        from .quality import validate_cases
        cases = value.get("cases") if isinstance(value, dict) else value
        if not isinstance(cases, list):
            raise ContractError("evolution suite must contain cases")
        try:
            return deepcopy(validate_cases(cases))
        except ValueError as exc:
            raise ContractError("evolution invalid suite: " + str(exc)) from exc

    def freeze_suite(self, ref, cases, *, split, independence_ref=""):
        ref = _text(ref, "suite reference")
        cases = self._cases(cases)
        if split not in {"development", "holdout"}:
            raise ContractError("evolution invalid suite split")
        if split == "holdout":
            _text(independence_ref, "holdout independence reference")
        entries = []
        for case in cases:
            text = case.get("input", {}).get("text") if isinstance(case.get("input"), dict) else None
            if not isinstance(text, str) or not text.strip():
                raise ContractError("evolution frozen case requires an actual input text")
            if case.get("split") and case["split"] != split:
                raise ContractError("evolution case split conflicts with frozen suite")
            entries.append({"case_id": case["case_id"], "case_hash": key(case),
                            "source_version": case["source_version"], "channel": case["channel"],
                            "input_hash": key(text.strip()), "split_group": case.get("split_group", ""),
                            "source_sha256": case.get("source_sha256", "")})
        manifest = {"suite_ref": ref, "suite_hash": key(cases), "split": split,
                    "independence_ref": independence_ref, "case_count": len(cases), "cases": entries,
                    "independence_kind": "host_attestation_and_overlap_checks_not_human_acceptance"}
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM r7_evo_suites WHERE owner_id=? AND suite_ref=?",
                             (self.owner_id, ref)).fetchone()
            if row:
                old = self._decoded(row, payload="manifest_json", digest="manifest_hash")
                if old != manifest:
                    raise ContractError("evolution frozen suite version conflict")
                return old
            db.execute("INSERT INTO r7_evo_suites VALUES(?,?,?,?,?)",
                       (self.owner_id, ref, dumps(manifest), key(manifest), self.store.clock()))
        return manifest

    def _manifest(self, ref):
        row = self.store.one("SELECT * FROM r7_evo_suites WHERE owner_id=? AND suite_ref=?",
                             (self.owner_id, ref))
        return self._decoded(row, payload="manifest_json", digest="manifest_hash")

    def _load_suite(self, frozen):
        if not callable(self.suite_loader):
            raise ContractError("evolution suite loader unbound")
        current = self._manifest(frozen["suite_ref"])
        if current != frozen:
            raise ContractError("evolution frozen suite manifest changed")
        cases = self._cases(self.suite_loader(frozen["suite_ref"]))
        if key(cases) != frozen["suite_hash"]:
            raise ContractError("evolution frozen suite content changed")
        return cases

    @staticmethod
    def _disjoint(development, holdout):
        if development["split"] != "development" or holdout["split"] != "holdout":
            raise ContractError("evolution suite roles must be development and holdout")
        for name in ("case_id", "input_hash", "split_group"):
            left = {c[name] for c in development["cases"] if c[name]}
            right = {c[name] for c in holdout["cases"] if c[name]}
            if left & right:
                raise ContractError("evolution contaminated holdout: overlapping " + name)
        if (development["suite_hash"] == holdout["suite_hash"]
                or development["suite_ref"] == holdout["suite_ref"]):
            raise ContractError("evolution development set is not a holdout")

    def create_candidate(self, *, scope, patch, development_ref, holdout_ref=None,
                         proposal_id="", proposal_hash="", evidence_refs=(), rationale="",
                         budget=None, expected_parent_version=None):
        scope, patch = _text(scope, "scope"), self._validate_config(patch, patch=True)
        active = self.active_config(scope)
        if active is None:
            raise ContractError("evolution baseline not registered")
        if expected_parent_version is not None and active["version"] != expected_parent_version:
            raise ContractError("evolution baseline changed during proposal")
        config = self._validate_config({**active["config"], **patch})
        if config == active["config"]:
            raise ContractError("evolution candidate has no configuration change")
        development = self._manifest(_text(development_ref, "development reference"))
        if development["split"] != "development":
            raise ContractError("evolution development suite required")
        holdout = self._manifest(_text(holdout_ref, "holdout reference")) if holdout_ref else None
        if holdout:
            self._disjoint(development, holdout)
        budget = (ReplayBudget(**budget) if isinstance(budget, dict) else budget or ReplayBudget()).validate()
        payload = {"schema_version": VERSION, "scope": scope, "parent_version": active["version"],
                   "parent_candidate_id": active["candidate_id"], "baseline_config": active["config"],
                   "config": config, "patch": patch, "proposal_id": proposal_id,
                   "proposal_hash": proposal_hash, "evidence_refs": list(evidence_refs),
                   "rationale": str(rationale), "development": development, "holdout": holdout,
                   "budget": asdict(budget), "evaluation_layer": self.layer,
                   "runtime_revision": self.runtime_revision,
                   "promotion_policy": self._policy_identity(scope),
                   "required_minimum_latency_gain": self.minimum_latency_gain}
        candidate_id = "ec_" + key(self.owner_id, payload)[:40]
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM r7_evo_candidates WHERE candidate_id=?", (candidate_id,)).fetchone()
            if row:
                if self._decoded(row) != payload:
                    raise ContractError("evolution candidate identity conflict")
                return {"candidate_id": candidate_id, "state": row["state"], **payload}
            db.execute("INSERT INTO r7_evo_candidates VALUES(?,?,?,?,?,?,?,?)",
                       (candidate_id, self.owner_id, scope, dumps(payload), key(payload),
                        "proposed", "", self.store.clock()))
        return {"candidate_id": candidate_id, "state": "proposed", **payload}

    def _journal_evidence(self, journal, proposal):
        observations, feedback = [], []
        plan = proposal.get("replay_plan", {})
        for identifier in plan.get("observation_ids", []):
            row = journal.store.one("SELECT * FROM r6_improvement_observations WHERE owner_id=? AND observation_id=?",
                                    (self.owner_id, identifier))
            observations.append(self._decoded(row))
        for identifier in plan.get("feedback_ids", []):
            row = journal.store.one("SELECT * FROM r6_improvement_feedback WHERE owner_id=? AND feedback_id=?",
                                    (self.owner_id, identifier))
            feedback.append(self._decoded(row))
        return observations, feedback

    def _record_proposal_attempt(self, proposal_id, state, detail, *, scope):
        payload = {"proposal_id": proposal_id, "scope": scope, "state": state, "detail": detail}
        with self.store.transaction() as db:
            db.execute("INSERT INTO r7_evo_proposal_attempts VALUES(?,?,?,?,?,?,?)",
                       ("ep_" + uuid.uuid4().hex, self.owner_id, proposal_id, state,
                        dumps(payload), key(payload), self.store.clock()))

    def propose_from_journal(self, journal, *, scope, development_ref, holdout_ref=None,
                             budget=None, proposal_id=None):
        if journal.owner_id != self.owner_id or journal.store.path != self.store.path:
            raise ContractError("evolution journal owner/store mismatch")
        missing = [name for name, port in (("proposer", self.proposer), ("suite_loader", self.suite_loader))
                   if not callable(port)]
        if missing:
            return {"state": "unbound", "missing_bindings": missing, "candidate_id": None}
        used = {json.loads(r["payload_json"]).get("proposal_id") for r in self.store.rows(
            "SELECT payload_json FROM r7_evo_candidates WHERE owner_id=? AND scope=?", (self.owner_id, scope))}
        # No-change feedback must not start another model call every background
        # tick. Failures retain all attempts and get at most three automatic
        # proposal tries; an explicit proposal_id can be retried by the host.
        attempt_counts = {}
        for row in self.store.rows("SELECT * FROM r7_evo_proposal_attempts WHERE owner_id=?", (self.owner_id,)):
            attempt = self._decoded(row)
            if attempt.get("scope") != scope:
                continue
            identifier = attempt["proposal_id"]
            attempt_counts[identifier] = attempt_counts.get(identifier, 0) + 1
            if attempt["state"] in {"proposed", "no_change"} or attempt_counts[identifier] >= 3:
                used.add(identifier)
        proposals = journal.list_proposals(limit=200)
        proposal = next((p for p in proposals if p["proposal_id"] == proposal_id), None) if proposal_id else next(
            (p for p in proposals if p["proposal_id"] not in used), None)
        if proposal is None:
            return {"state": "idle", "candidate_id": None, "reason": "no_unprocessed_proposal"}
        active = self.active_config(scope)
        if not active:
            raise ContractError("evolution baseline not registered")
        development = self._manifest(development_ref)
        if development["split"] != "development":
            raise ContractError("evolution proposer can only read development cases")
        cases = self._load_suite(development)
        observations, feedback = self._journal_evidence(journal, proposal)
        limits = (ReplayBudget(**budget) if isinstance(budget, dict) else budget or ReplayBudget()).validate()
        context = {"mode": "proposal", "effects_allowed": False, "scope": scope,
                   "proposal_id": proposal["proposal_id"], "timeout_seconds": limits.per_call_seconds}
        try:
            self._call_boundary(context)
        except ReplayDeferred as exc:
            # No proposal call has been reserved or submitted, so yielding is
            # neither a failed model attempt nor a consumed proposal retry.
            return {"state": "deferred", "candidate_id": None, "proposal_id": proposal["proposal_id"],
                    "reason": exc.reason, "proposal_model_called": False,
                    "call_submitted_at_checkpoint": False, "evidence_state": "not_started",
                    "step_calls_used": 0, "active_seconds_this_step": 0.0}
        timeout_seconds = context["timeout_seconds"]
        began = self.monotonic()
        try:
            proposed = self.proposer(proposal=deepcopy(proposal), observations=observations,
                feedback=feedback, baseline_config=deepcopy(active["config"]),
                development_cases=cases, allowed_config_keys=sorted(self.allowed_config_keys),
                known_model_ids=sorted(self.known_model_ids), timeout_seconds=timeout_seconds)
        except Exception as exc:
            self._record_proposal_attempt(proposal["proposal_id"], "failed", {"error_type": type(exc).__name__}, scope=scope)
            return {"state": "proposal_failed", "candidate_id": None, "reason": type(exc).__name__}
        if self.monotonic() - began > timeout_seconds:
            self._record_proposal_attempt(proposal["proposal_id"], "timeout", {"physical_cancellation_proven": False}, scope=scope)
            return {"state": "proposal_timeout", "candidate_id": None,
                    "physical_cancellation_proven": False}
        if not isinstance(proposed, dict) or not isinstance(proposed.get("patch"), dict):
            self._record_proposal_attempt(proposal["proposal_id"], "rejected", {"reason": "invalid_patch_contract"}, scope=scope)
            return {"state": "proposal_rejected", "candidate_id": None, "reason": "invalid_patch_contract"}
        if not proposed["patch"] or {**active["config"], **proposed["patch"]} == active["config"]:
            self._record_proposal_attempt(proposal["proposal_id"], "no_change", {"rationale": str(proposed.get("rationale", ""))}, scope=scope)
            return {"state": "no_change", "candidate_id": None, "improvement_verified": False}
        # Pin the baseline seen by the proposer; never silently rebase onto a
        # concurrently activated version.
        if self.active_config(scope)["version"] != active["version"]:
            raise ContractError("evolution baseline changed during proposal")
        try:
            candidate = self.create_candidate(scope=scope, patch=proposed["patch"],
                development_ref=development_ref, holdout_ref=holdout_ref,
                proposal_id=proposal["proposal_id"], proposal_hash=key(proposal),
                evidence_refs=proposal.get("evidence_refs", []), rationale=proposed.get("rationale", ""),
                budget=limits, expected_parent_version=active["version"])
        except ContractError as exc:
            self._record_proposal_attempt(proposal["proposal_id"], "rejected", {"reason": str(exc)}, scope=scope)
            return {"state": "proposal_rejected", "candidate_id": None, "reason": str(exc)}
        self._record_proposal_attempt(proposal["proposal_id"], "proposed", {"candidate_id": candidate["candidate_id"]}, scope=scope)
        return candidate

    def _candidate(self, candidate_id, db=None):
        query = "SELECT * FROM r7_evo_candidates WHERE owner_id=? AND candidate_id=?"
        args = (self.owner_id, candidate_id)
        row = db.execute(query, args).fetchone() if db else self.store.one(query, args)
        payload = self._decoded(row)
        self._validate_config(payload["config"])
        return row, payload

    @staticmethod
    def _empty_record(case, context, status, reason):
        from .quality import SCHEMA_VERSION
        return {"schema_version": SCHEMA_VERSION, "case_id": case["case_id"],
                "channel": case["channel"], "intent": case["intent"],
                "source_version": case["source_version"], "attempt_id": context["attempt_id"],
                "layer": context["layer"], "outcome": {"status": status}, "reason": reason,
                "output": {"text": "", "kind": "answer"},
                "execution_provenance": {"actual_assistant_execution_observed": False}}

    @staticmethod
    def _worker_case(case):
        # Never hand expected answers, semantic oracles or human ratings to the
        # model worker. A trusted host verifier can read its source separately.
        fields = ("case_id", "channel", "source_version", "input", "fixture_ref",
                  "source_ref", "environment_ref", "setup", "cold_or_warm")
        return {name: deepcopy(case[name]) for name in fields if name in case}

    def _active_seconds(self, counter, began):
        elapsed = self.monotonic() - began
        if not _finite(elapsed) or elapsed < 0:
            raise ContractError("evolution active replay clock moved backwards")
        return counter["previous_active_seconds"] + elapsed

    def _assert_progress_owner(self, db, run_id, counter):
        progress = db.execute("SELECT * FROM r8_evo_replay_progress WHERE run_id=? AND owner_id=?",
                              (run_id, self.owner_id)).fetchone()
        if not progress or progress["state"] != "running" or progress["epoch"] != counter["epoch"]:
            raise ContractError("evolution replay epoch ownership changed")
        return progress

    def _existing_trial(self, run_id, split, variant, case, attempt_id):
        values = (run_id, split, variant, case["case_id"], attempt_id)
        attempt = self.store.one("""SELECT * FROM r8_evo_attempts
            WHERE run_id=? AND split=? AND variant=? AND case_id=? AND attempt_id=?""", values)
        saved = self.store.one("""SELECT * FROM r7_evo_trials
            WHERE run_id=? AND split=? AND variant=? AND case_id=? AND attempt_id=?""", values)
        if not saved and not attempt:
            return None
        if not saved or not attempt or attempt["state"] != "done":
            raise ContractError("evolution attempt outcome unresolved; no automatic resubmission")
        record = self._decoded(saved, payload="record_json", digest="record_hash")
        if attempt["record_hash"] != saved["record_hash"]:
            raise ContractError("evolution completed attempt evidence mismatch")
        return record

    def _start_attempt(self, run_id, split, variant, case, attempt_id, counter, began, timeout_seconds):
        """Reserve the call durably before invoking an external worker."""
        elapsed = self._active_seconds(counter, began)
        with self.store.transaction() as db:
            progress = self._assert_progress_owner(db, run_id, counter)
            if progress["calls_used"] != counter["calls"]:
                raise ContractError("evolution replay call accounting changed")
            db.execute("""INSERT INTO r8_evo_attempts(run_id,split,variant,case_id,attempt_id,
                state,epoch,started,reserved_seconds) VALUES(?,?,?,?,?,'started',?,?,?)""",
                       (run_id, split, variant, case["case_id"], attempt_id,
                        counter["epoch"], self.store.clock(), timeout_seconds))
            db.execute("""UPDATE r8_evo_replay_progress
                SET calls_used=calls_used+1,active_seconds=?,updated=? WHERE run_id=?""",
                       (elapsed, self.store.clock(), run_id))
        counter["calls"] += 1

    def _save_trial(self, run_id, split, variant, case, attempt_id, record, *, counter, began,
                    submitted=False, duration=None, outcome_unresolved=False):
        elapsed = self._active_seconds(counter, began)
        with self.store.transaction() as db:
            self._assert_progress_owner(db, run_id, counter)
            values = (run_id, split, variant, case["case_id"], attempt_id)
            if submitted:
                attempt = db.execute("""SELECT * FROM r8_evo_attempts
                    WHERE run_id=? AND split=? AND variant=? AND case_id=? AND attempt_id=?""",
                    values).fetchone()
                if not attempt or attempt["state"] != "started" or attempt["epoch"] != counter["epoch"]:
                    raise ContractError("evolution attempt ownership changed")
            db.execute("INSERT INTO r7_evo_trials VALUES(?,?,?,?,?,?,?,?)",
                       (*values, dumps(record), key(record), self.store.clock()))
            if submitted:
                # A persisted response can itself report physical execution
                # unknown. Retain it atomically without falsely marking done.
                db.execute("""UPDATE r8_evo_attempts SET state=?,finished=?,
                    elapsed_seconds=?,record_hash=?
                    WHERE run_id=? AND split=? AND variant=? AND case_id=? AND attempt_id=?""",
                    ('unknown' if outcome_unresolved else 'done',
                     self.store.clock(), duration, key(record), *values))
            db.execute("UPDATE r8_evo_replay_progress SET active_seconds=?,updated=? WHERE run_id=?",
                       (elapsed, self.store.clock(), run_id))

    def _call_boundary(self, context):
        if self.should_defer is not None:
            defer = self.should_defer(deepcopy(context))
            if type(defer) is not bool:
                raise ContractError("evolution should_defer must return a boolean")
            if defer:
                raise ReplayDeferred("host_requested_yield_before_submission")
        if self.host_remaining_seconds is not None:
            remaining = self.host_remaining_seconds()
            if type(remaining) not in (int, float) or math.isnan(remaining) or remaining < 0:
                raise ContractError("evolution invalid host remaining budget")
            if remaining == 0:
                raise ReplayDeferred("host_budget_exhausted_before_submission")
            context["timeout_seconds"] = min(context["timeout_seconds"], remaining)

    def _run_split(self, run_id, candidate, split, cases, counter, began):
        from .quality import compare_quality
        budget = ReplayBudget(**candidate["budget"])
        records = {"baseline": [], "candidate": []}
        for case_index, case in enumerate(cases):
            for repeat in range(budget.repetitions):
                attempt_id = "repeat-" + str(repeat + 1)
                # Alternate order to reduce a systematic cache/warm-up advantage.
                order = ("baseline", "candidate") if (case_index + repeat) % 2 == 0 else ("candidate", "baseline")
                for variant in order:
                    previous = self._existing_trial(run_id, split, variant, case, attempt_id)
                    if previous is not None:
                        records[variant].append(previous)
                        continue
                    elapsed = self._active_seconds(counter, began)
                    remaining = budget.max_seconds - elapsed
                    context = {"mode": "evaluation", "effects_allowed": False, "run_id": run_id,
                        "variant": variant, "split": split, "layer": candidate["evaluation_layer"],
                        "attempt_id": attempt_id, "case_id": case["case_id"], "intent": case["intent"],
                        "source_version": case["source_version"], "channel": case["channel"],
                        "timeout_seconds": min(budget.per_call_seconds, max(0.0, remaining)),
                        "remaining_calls": max(0, budget.max_calls - counter["calls"])}
                    submitted, duration, outcome_unresolved = False, None, False
                    if remaining <= 0 or counter["calls"] >= budget.max_calls:
                        counter["exhausted"] = True
                        record = self._empty_record(case, context, "not_run", "replay_budget_exhausted")
                    else:
                        # This hook is deliberately outside the worker try block:
                        # a callback inside a submitted worker cannot claim that
                        # no model request was ever started.
                        self._call_boundary(context)
                        remaining = budget.max_seconds - self._active_seconds(counter, began)
                        if remaining <= 0:
                            counter["exhausted"] = True
                            record = self._empty_record(case, context, "not_run", "replay_budget_exhausted")
                            self._save_trial(run_id, split, variant, case, attempt_id, record,
                                             counter=counter, began=began)
                            records[variant].append(record)
                            continue
                        context["timeout_seconds"] = min(context["timeout_seconds"], remaining)
                        self._start_attempt(run_id, split, variant, case, attempt_id, counter, began,
                                            context["timeout_seconds"])
                        submitted = True
                        started = self.monotonic()
                        config = candidate["baseline_config"] if variant == "baseline" else candidate["config"]
                        try:
                            record = self.replayer(case=self._worker_case(case), config=deepcopy(config),
                                                   context=deepcopy(context))
                            record = deepcopy(record)
                            if not isinstance(record, dict):
                                raise ContractError("worker did not return a trial")
                            dumps(record)
                            outcome_unresolved = (isinstance(record.get('outcome'), dict)
                                and record['outcome'].get('status') == 'unknown')
                            if outcome_unresolved:
                                # Later identity/timeout rejection must not erase
                                # the worker's physical execution uncertainty.
                                record['execution_outcome_unknown'] = True
                                record['reported_outcome'] = deepcopy(record['outcome'])
                        except Exception as exc:
                            record = self._empty_record(case, context, "error", "worker_error:" + type(exc).__name__)
                        duration = self.monotonic() - started
                        if duration < 0 or duration > context["timeout_seconds"]:
                            record = {**record, "outcome": {"status": "timeout"},
                                      "reason": "worker_returned_after_budget",
                                      "physical_cancellation_proven": False}
                        # Preserve mismatched output as failed evidence. Do not
                        # rewrite a foreign worker result into the planned case.
                        if any(record.get(field) != context[field] for field in (
                                "case_id", "channel", "source_version", "attempt_id", "layer")):
                            record["outcome"] = {"status": "error"}
                            record["reason"] = "worker_identity_mismatch"
                        if case.get("source_sha256") and record.get("source_sha256") != case["source_sha256"]:
                            record["outcome"] = {"status": "error"}
                            record["reason"] = "worker_source_snapshot_mismatch"
                    self._save_trial(run_id, split, variant, case, attempt_id, record,
                                     counter=counter, began=began, submitted=submitted, duration=duration,
                                     outcome_unresolved=outcome_unresolved)
                    records[variant].append(record)
                    if outcome_unresolved:
                        raise ContractError("replayer_reported_execution_unknown")
        result = compare_quality(cases, records["baseline"], records["candidate"],
                                 layers=(candidate["evaluation_layer"],))
        result["expected_pairs"] = len(cases) * budget.repetitions
        return result

    @staticmethod
    def _quality_gate(comparison):
        return (comparison["candidate"]["ci_pass"] and not comparison["baseline"]["invalid_records"]
                and len(comparison["paired"]) == comparison["expected_pairs"]
                and all(pair["same_conditions"] for pair in comparison["paired"]))

    @staticmethod
    def _relative_evidence_reasons(comparison, split):
        """A relative speed gain cannot hide missing evidence or channel loss.

        Both variants must be complete, correct and source-verified. Improving
        a known incorrect baseline first requires repairing and verifying that
        baseline; unknown/error results are never a conveniently slow baseline.
        """
        from .quality import distribution
        reasons, strata = [], {}
        for variant in ("baseline", "candidate"):
            result = comparison[variant]
            if not result.get("ci_pass") or result.get("covers_all_required_layers") is not True:
                reasons.append(split + "_" + variant + "_evidence_incomplete")
            for row in result["results"]:
                record = row["record"]
                provenance = record.get("execution_provenance", {})
                if (provenance.get("actual_model_call") is not True
                        or not isinstance(provenance.get("model_run_ref"), str)
                        or not provenance["model_run_ref"].strip()):
                    reasons.append(split + "_" + variant + "_actual_model_evidence_missing")
                seconds = row["timing"].get("completion_s")
                if not _finite(seconds) or row["timing"].get("issues"):
                    reasons.append(split + "_" + variant + "_timing_evidence_missing")
                    continue
                label = (row["channel"], row["cold_or_warm"])
                strata.setdefault(label, {"baseline": [], "candidate": []})[variant].append(seconds)
        # Group/private and cold/warm populations remain separate. An overall
        # gain must not be bought by making one measured population slower.
        for (channel, thermal), values in strata.items():
            a, b = distribution(values["baseline"]), distribution(values["candidate"])
            if a["n"] != b["n"] or not a["n"]:
                reasons.append(split + "_stratum_pairing_incomplete:" + channel + ":" + thermal)
            elif b["p50"] > a["p50"] or (a["p95"] is not None and b["p95"] > a["p95"]):
                reasons.append(split + "_stratum_latency_regression:" + channel + ":" + thermal)
        return reasons

    @staticmethod
    def _relative_style_reasons(comparison, split):
        reasons = []
        baseline = {(r["case_id"], r["attempt_id"]): r for r in comparison["baseline"]["results"]}
        for row in comparison["candidate"]["results"]:
            previous = baseline.get((row["case_id"], row["attempt_id"]))
            if previous is None:
                reasons.append(split + "_style_pair_missing")
                continue
            if set(row["style"]["flags"]) - set(previous["style"]["flags"]):
                reasons.append(split + "_style_rule_regression")
            before, after = previous["style"]["human_score"], row["style"]["human_score"]
            if before is not None and after is not None and after < before:
                reasons.append(split + "_human_style_regression")
        return reasons

    def _decision(self, candidate, comparisons, counter):
        reasons, improvements = [], []
        result = {"eligible": False, "state": "rejected", "reasons": reasons, "improvements": improvements,
                  "style_acceptance": "not_inferred_from_automatic_replay", "model_weights_changed": False,
                  "business_effects_applied": False, "production_deployment_proven": False,
                  "promotion_policy": self._check_candidate_policy(candidate),
                  "release_gate_pass": False, "local_improvement_eligible": False,
                  "evidence_state": "rejected", "product_latency_target_status": "unknown"}
        if counter["exhausted"]:
            result.update(state="budget_exhausted", evidence_state="incomplete")
            reasons.append("planned_replay_not_completed")
            return result
        development = comparisons.get("development")
        if not development or not self._quality_gate(development):
            reasons.append("development_correctness_or_pairing_failed")
            return result
        if development["candidate"].get("covers_all_required_layers") is False:
            result.update(state="needs_evidence", evidence_state="insufficient")
            reasons.append("development_required_layers_unverified")
            return result
        if candidate["holdout"] is None:
            result.update(state="needs_holdout", evidence_state="insufficient")
            reasons.append("independent_frozen_holdout_not_supplied")
            return result
        holdout = comparisons.get("holdout")
        if not holdout or not self._quality_gate(holdout):
            reasons.append("holdout_correctness_or_pairing_failed")
            return result
        if MODEL_KEYS.intersection(candidate["patch"]) and candidate["evaluation_layer"] == "offline":
            result.update(state="needs_evidence", evidence_state="insufficient")
            reasons.append("model_setting_change_requires_actual_model_replay")
            return result
        layer = candidate["evaluation_layer"]
        baseline, updated = holdout["baseline"], holdout["candidate"]
        a, b = baseline["layers"][layer], updated["layers"][layer]
        if updated.get("covers_all_required_layers") is False:
            reasons.append("candidate_required_layers_unverified")
        elif (self.promotion_policy == "absolute_release" and not updated["release_gate_pass"]):
            reasons.append("candidate_frozen_latency_targets_not_met")
        result["release_gate_pass"] = bool(development["candidate"]["release_gate_pass"]
                                             and updated["release_gate_pass"])
        target_states = {row["timing"]["target_status"]
                         for comparison in (development, holdout)
                         for row in comparison["candidate"]["results"]}
        result["product_latency_target_status"] = (
            "missed" if "missed" in target_states else
            "unknown" if "unknown" in target_states or not target_states else "met")
        if self.promotion_policy == "relative_quality":
            for split, comparison in (("development", development), ("holdout", holdout)):
                reasons.extend(self._relative_evidence_reasons(comparison, split))
                reasons.extend(self._relative_style_reasons(comparison, split))
        old_p95, new_p95 = a["correct_completion_s"]["p95"], b["correct_completion_s"]["p95"]
        result["latency"] = {"baseline": a["correct_completion_s"], "candidate": b["correct_completion_s"],
                             "statistical_significance_claimed": False}
        if new_p95 is None:
            reasons.append("holdout_p95_samples_insufficient")
        elif old_p95 is not None and new_p95 > old_p95:
            reasons.append("holdout_p95_regression")
        old_success = a["case_pass_count"]
        if b["case_pass_count"] > old_success:
            improvements.append("correctness_or_task_completion")
        elif old_p95 is None:
            reasons.append("baseline_p95_samples_insufficient")
        if (old_p95 is not None and new_p95 is not None and new_p95 < old_p95
                and new_p95 <= old_p95 * (1 - candidate["required_minimum_latency_gain"])):
            improvements.append("measured_holdout_p95")
        # Rules and separately sourced human opinions stay distinct. The count
        # of warnings is a proxy and never a user's acceptance of the style.
        old_rows = {(r["case_id"], r["attempt_id"]): r for r in baseline["results"]}
        removed_flags, human_gain = 0, 0
        for new in updated["results"]:
            old = old_rows.get((new["case_id"], new["attempt_id"]))
            if not old:
                reasons.append("style_pair_missing"); continue
            flags_old, flags_new = set(old["style"]["flags"]), set(new["style"]["flags"])
            if flags_new - flags_old:
                reasons.append("style_rule_regression")
            removed_flags += len(flags_old - flags_new)
            sa, sb = old["style"]["human_score"], new["style"]["human_score"]
            if sa is not None and sb is not None:
                if sb < sa:
                    reasons.append("human_style_regression")
                human_gain += sb > sa
        if removed_flags:
            improvements.append("style_rule_proxy")
        if human_gain:
            improvements.append("paired_human_style_scores")
        if not improvements:
            reasons.append("no_measured_improvement")
        result["reasons"] = list(dict.fromkeys(reasons))
        if not reasons:
            result.update(eligible=True, local_improvement_eligible=True, state="eligible",
                          evidence_state="verified_paired_replay")
        elif any("insufficient" in reason or "layers_unverified" in reason
                 or "evidence_" in reason or "pairing_incomplete" in reason for reason in reasons):
            result["state"] = "needs_evidence"
            result["evidence_state"] = "insufficient"
        else:
            result["evidence_state"] = "rejected"
        return result

    def replay(self, candidate_id):
        row, candidate = self._candidate(_text(candidate_id, "candidate_id"))
        if candidate["runtime_revision"] != self.runtime_revision:
            raise ContractError("evolution replay runtime changed; create a new candidate")
        policy = self._check_candidate_policy(candidate)
        missing = [name for name, port in (("replayer", self.replayer), ("suite_loader", self.suite_loader))
                   if not callable(port)]
        if missing:
            return {"state": "unbound", "candidate_id": candidate_id, "eligible": False,
                    "missing_bindings": missing, "comparisons": {}, "business_effects_applied": False,
                    "promotion_policy": policy, "evidence_state": "binding_missing",
                    "release_gate_pass": False, "local_improvement_eligible": False}
        # Verify all manifests before spending model calls. Held-out contents are
        # loaded only after development has passed.
        development = self._load_suite(candidate["development"])
        if candidate["holdout"]:
            if self._manifest(candidate["holdout"]["suite_ref"]) != candidate["holdout"]:
                raise ContractError("evolution frozen holdout manifest changed")
        run_id, initial_calls, previous_active, epoch = "er_" + uuid.uuid4().hex, 0, 0.0, 1
        with self.store.transaction() as db:
            current, _ = self._candidate(candidate_id, db)
            if current["state"] == "unknown":
                old_run = db.execute("SELECT * FROM r7_evo_runs WHERE run_id=? AND owner_id=?",
                                     (current["active_run_id"], self.owner_id)).fetchone()
                report = self._decoded(old_run, payload="report_json", digest="report_hash")
                return {**report, "step_calls_used": 0, "active_seconds_this_step": 0.0,
                        "repeated_status": True}
            if current["state"] in {"replaying", "deferred"}:
                old_run = db.execute("SELECT * FROM r7_evo_runs WHERE run_id=?", (current["active_run_id"],)).fetchone()
                progress = db.execute("SELECT * FROM r8_evo_replay_progress WHERE run_id=? AND owner_id=?",
                                      (current["active_run_id"], self.owner_id)).fetchone()
                if not old_run:
                    raise ContractError("evolution active run record missing")
                stale_after = candidate["budget"]["max_seconds"] + candidate["budget"]["per_call_seconds"]
                updated = progress["updated"] if progress else old_run["started"]
                if current["state"] == "replaying":
                    if self.store.clock() <= updated + stale_after:
                        return {"state": "busy", "candidate_id": candidate_id, "run_id": old_run["run_id"],
                                "eligible": False, "step_calls_used": 0, "active_seconds_this_step": 0.0}
                    # A dead process may have submitted a model call immediately
                    # before it died. Never turn lease expiry into permission to
                    # resubmit that input, even when a done result is absent.
                    unknown = {"schema_version": VERSION, "state": "unknown", "eligible": False,
                        "candidate_id": candidate_id, "run_id": old_run["run_id"],
                        "reason": "abandoned_replay_requires_reconciliation",
                        "calls_used": progress["calls_used"] if progress else None,
                        "active_seconds_used": progress["active_seconds"] if progress else None,
                        "step_calls_used": 0, "active_seconds_this_step": 0.0,
                        "physical_cancellation_proven": False, "automatic_retry_allowed": False,
                        "evidence_state": "outcome_unknown", "local_improvement_eligible": False,
                        "release_gate_pass": False, "promotion_policy": policy}
                    reserved = db.execute("""SELECT COALESCE(SUM(reserved_seconds),0) AS n
                        FROM r8_evo_attempts WHERE run_id=? AND state IN ('started','unknown')""", (old_run["run_id"],)).fetchone()["n"]
                    unknown.update(active_seconds_complete=False,
                                   reserved_seconds_for_unresolved=reserved,
                                   budget_seconds_charged=(progress["active_seconds"] + reserved) if progress else None)
                    db.execute("UPDATE r8_evo_attempts SET state='unknown' WHERE run_id=? AND state='started'",
                               (old_run["run_id"],))
                    db.execute("UPDATE r8_evo_replay_progress SET state='unknown',epoch=epoch+1,updated=? WHERE run_id=?",
                               (self.store.clock(), old_run["run_id"]))
                    db.execute("UPDATE r7_evo_runs SET state='unknown',finished=?,report_json=?,report_hash=? WHERE run_id=?",
                               (self.store.clock(), dumps(unknown), key(unknown), old_run["run_id"]))
                    db.execute("UPDATE r7_evo_candidates SET state='unknown' WHERE candidate_id=?", (candidate_id,))
                    return unknown
                if not progress or progress["state"] != "deferred" or old_run["state"] != "deferred":
                    raise ContractError("evolution deferred checkpoint missing")
                checkpoint = self._decoded(old_run, payload="report_json", digest="report_hash")
                if (checkpoint.get("promotion_policy") != policy
                        or checkpoint.get("calls_used") != progress["calls_used"]
                        or checkpoint.get("active_seconds_used") != progress["active_seconds"]):
                    raise ContractError("evolution deferred checkpoint accounting mismatch")
                attempts = db.execute("SELECT state,COUNT(*) AS n FROM r8_evo_attempts WHERE run_id=? GROUP BY state",
                                      (old_run["run_id"],)).fetchall()
                counts = {entry["state"]: entry["n"] for entry in attempts}
                saved_count = db.execute("SELECT COUNT(*) AS n FROM r7_evo_trials WHERE run_id=?",
                                         (old_run["run_id"],)).fetchone()["n"]
                if (any(state != "done" for state in counts)
                        or counts.get("done", 0) != progress["calls_used"] or saved_count != progress["calls_used"]):
                    raise ContractError("evolution deferred attempt unresolved; no automatic resubmission")
                run_id, initial_calls = old_run["run_id"], progress["calls_used"]
                previous_active, epoch = progress["active_seconds"], progress["epoch"] + 1
                db.execute("UPDATE r8_evo_replay_progress SET state='running',epoch=?,updated=? WHERE run_id=?",
                           (epoch, self.store.clock(), run_id))
                db.execute("UPDATE r7_evo_runs SET state='running',finished=NULL WHERE run_id=?", (run_id,))
                self.store.audit(db, "r8_evolution_resumed", run_id, "epoch=" + str(epoch))
            else:
                db.execute("INSERT INTO r7_evo_runs(run_id,owner_id,candidate_id,state,started) VALUES(?,?,?,'running',?)",
                           (run_id, self.owner_id, candidate_id, self.store.clock()))
                db.execute("INSERT INTO r8_evo_replay_progress VALUES(?,?,?,0,0,1,'running',?)",
                           (run_id, self.owner_id, candidate_id, self.store.clock()))
            db.execute("UPDATE r7_evo_candidates SET state='replaying',active_run_id=? WHERE candidate_id=?",
                       (run_id, candidate_id))
        began, comparisons = self.monotonic(), {}
        counter = {"calls": initial_calls, "exhausted": False, "epoch": epoch,
                   "previous_active_seconds": previous_active}
        try:
            comparisons["development"] = self._run_split(run_id, candidate, "development", development, counter, began)
            self._load_suite(candidate["development"])
            if candidate["holdout"]:
                # A bad development candidate never consumes private holdout
                # calls. Missing planned evidence remains explicit in the report.
                development_layers_complete = comparisons["development"]["candidate"].get(
                    "covers_all_required_layers") is not False
                if (self._quality_gate(comparisons["development"]) and development_layers_complete
                        and not counter["exhausted"]):
                    holdout = self._load_suite(candidate["holdout"])
                    comparisons["holdout"] = self._run_split(run_id, candidate, "holdout", holdout, counter, began)
                    self._load_suite(candidate["holdout"])
                else:
                    comparisons["holdout_not_run_reason"] = (
                        "development_required_layers_unverified" if not development_layers_complete
                        else "development_gate_or_budget")
            if self._active_seconds(counter, began) > candidate["budget"]["max_seconds"]:
                counter["exhausted"] = True
            decision = self._decision(candidate, comparisons, counter)
            previous_failures = self.store.rows("""SELECT run_id,state FROM r7_evo_runs
                WHERE owner_id=? AND candidate_id=? AND run_id<>?
                AND state IN ('rejected','failed','budget_exhausted','interrupted','unknown') ORDER BY started,run_id""",
                (self.owner_id, candidate_id, run_id))
            if previous_failures:
                decision["prior_failed_runs"] = previous_failures
                if decision.get("eligible"):
                    decision.update(state="rejected", eligible=False, local_improvement_eligible=False,
                                    evidence_state="rejected")
                    decision["reasons"].append("same_candidate_has_prior_failed_attempts")
        except ReplayDeferred as exc:
            decision = {"state": "deferred", "eligible": False, "reason": exc.reason,
                        "evidence_state": "checkpointed_incomplete", "resume_supported": True,
                        "call_submitted_at_checkpoint": False, "release_gate_pass": False,
                        "local_improvement_eligible": False, "business_effects_applied": False,
                        "production_deployment_proven": False}
        except Exception as exc:
            decision = {"state": "failed", "eligible": False, "reasons": [type(exc).__name__ + ": " + str(exc)],
                        "business_effects_applied": False, "production_deployment_proven": False,
                        "evidence_state": "failed", "release_gate_pass": False,
                        "local_improvement_eligible": False}
        active_seconds = self._active_seconds(counter, began)
        report = {"schema_version": VERSION, "candidate_id": candidate_id, "run_id": run_id,
                  "layer": candidate["evaluation_layer"], "calls_used": counter["calls"],
                  "step_calls_used": counter["calls"] - initial_calls,
                  "active_seconds_used": active_seconds,
                  "active_seconds_this_step": active_seconds - previous_active,
                  "active_seconds_complete": True, "budget_seconds_charged": active_seconds,
                  "reserved_seconds_for_unresolved": 0.0,
                  "comparisons": comparisons, "promotion_policy": policy, **decision}
        with self.store.transaction() as db:
            current, _ = self._candidate(candidate_id, db)
            if current["active_run_id"] != run_id or current["state"] != "replaying":
                raise ContractError("evolution replay ownership changed; late result retained only")
            self._assert_progress_owner(db, run_id, counter)
            unresolved = db.execute("SELECT COUNT(*) AS n FROM r8_evo_attempts WHERE run_id=? AND state IN ('started','unknown')",
                                    (run_id,)).fetchone()["n"]
            if unresolved:
                reserved = db.execute("""SELECT COALESCE(SUM(reserved_seconds),0) AS n FROM r8_evo_attempts
                    WHERE run_id=? AND state IN ('started','unknown')""", (run_id,)).fetchone()["n"]
                reported_unknown = db.execute("SELECT 1 FROM r8_evo_attempts WHERE run_id=? AND state='unknown' LIMIT 1",
                                              (run_id,)).fetchone() is not None
                report.update(state="unknown", eligible=False, local_improvement_eligible=False,
                              evidence_state="outcome_unknown", automatic_retry_allowed=False,
                              physical_cancellation_proven=False, release_gate_pass=False,
                              active_seconds_complete=False, reserved_seconds_for_unresolved=reserved,
                              budget_seconds_charged=active_seconds + reserved,
                              reason="replayer_reported_execution_unknown" if reported_unknown
                                  else "submitted_attempt_result_not_atomically_saved")
                db.execute("UPDATE r8_evo_attempts SET state='unknown' WHERE run_id=? AND state='started'", (run_id,))
            db.execute("UPDATE r7_evo_runs SET state=?,finished=?,report_json=?,report_hash=? WHERE run_id=?",
                       (report["state"], None if report["state"] == "deferred" else self.store.clock(),
                        dumps(report), key(report), run_id))
            db.execute("UPDATE r7_evo_candidates SET state=? WHERE candidate_id=?", (report["state"], candidate_id))
            db.execute("UPDATE r8_evo_replay_progress SET state=?,active_seconds=?,updated=? WHERE run_id=?",
                       (report["state"], active_seconds, self.store.clock(), run_id))
            if report["state"] == "deferred":
                self.store.audit(db, "r8_evolution_deferred", run_id, report["reason"])
        return report

    def activate(self, candidate_id, *, expected_version):
        if type(expected_version) is not int:
            raise ContractError("evolution expected_version must be an integer")
        _, candidate = self._candidate(candidate_id)
        if candidate["runtime_revision"] != self.runtime_revision:
            raise ContractError("evolution activation runtime changed")
        policy = self._check_candidate_policy(candidate)
        if candidate["scope"] not in self.enabled_scopes:
            return {"state": "shadow_only", "candidate_id": candidate_id, "activated": False,
                    "reason": "host_scope_not_enabled"}
        self._load_suite(candidate["development"])
        if not candidate["holdout"]:
            raise ContractError("evolution no frozen holdout")
        self._load_suite(candidate["holdout"])
        with self.store.transaction() as db:
            row, candidate = self._candidate(candidate_id, db)
            run = db.execute("SELECT * FROM r7_evo_runs WHERE run_id=? AND owner_id=?",
                             (row["active_run_id"], self.owner_id)).fetchone()
            if not run or not run["report_json"]:
                raise ContractError("evolution candidate has no completed replay")
            report = self._decoded(run, payload="report_json", digest="report_hash")
            if report.get("promotion_policy") != policy:
                # Legacy reports omitted this field and are valid only under
                # the original absolute policy, never a newly relaxed gate.
                if not ("promotion_policy" not in candidate and "promotion_policy" not in report
                        and policy["mode"] == "absolute_release"):
                    raise ContractError("evolution replay promotion policy does not match candidate")
            if row["state"] != "eligible" or report.get("eligible") is not True or run["state"] != "eligible":
                raise ContractError("evolution candidate did not pass comparison")
            if any(report.get("comparisons", {}).get(split, {}).get("candidate", {}).get(
                    "covers_all_required_layers") is not True for split in ("development", "holdout")):
                raise ContractError("evolution required replay layers not verified")
            active = db.execute("SELECT version FROM r7_evo_active WHERE owner_id=? AND scope=?",
                                (self.owner_id, candidate["scope"])).fetchone()
            if (not active or active["version"] != expected_version
                    or candidate["parent_version"] != expected_version):
                raise ContractError("evolution activation version conflict")
            previous = self._version(candidate["scope"], expected_version, db)
            if previous["config"] != candidate["baseline_config"]:
                raise ContractError("evolution baseline configuration changed")
            version = expected_version + 1
            payload = {"scope": candidate["scope"], "version": version, "parent_version": expected_version,
                       "config": candidate["config"], "candidate_id": candidate_id,
                       "reason": "host_enabled_scope_and_frozen_paired_replay", "run_id": run["run_id"],
                       "promotion_policy": policy, "release_gate_pass": report.get("release_gate_pass", False),
                       "product_latency_target_status": report.get("product_latency_target_status", "unknown")}
            db.execute("INSERT INTO r7_evo_versions VALUES(?,?,?,?,?,?,?,?)",
                       (self.owner_id, candidate["scope"], version, expected_version, dumps(payload),
                        key(payload), candidate_id, self.store.clock()))
            db.execute("UPDATE r7_evo_active SET version=? WHERE owner_id=? AND scope=?",
                       (version, self.owner_id, candidate["scope"]))
            if previous["candidate_id"]:
                db.execute("UPDATE r7_evo_candidates SET state='superseded' WHERE candidate_id=? AND state='active'",
                           (previous["candidate_id"],))
            db.execute("UPDATE r7_evo_candidates SET state='active' WHERE candidate_id=?", (candidate_id,))
            self.store.audit(db, "r7_configuration_activated", candidate_id, "scope=" + candidate["scope"])
        return {"state": "configuration_active", "activated": True, "production_deployment_proven": False, **payload}

    def rollback(self, scope, *, expected_version, reason, target_version=None):
        scope, reason = _text(scope, "scope"), _text(reason, "rollback reason")
        if scope not in self.enabled_scopes:
            return {"state": "shadow_only", "activated": False, "reason": "host_scope_not_enabled"}
        if type(expected_version) is not int or expected_version < 2:
            raise ContractError("evolution rollback requires an active changed version")
        with self.store.transaction() as db:
            active = db.execute("SELECT version FROM r7_evo_active WHERE owner_id=? AND scope=?",
                                (self.owner_id, scope)).fetchone()
            if not active or active["version"] != expected_version:
                raise ContractError("evolution rollback version conflict")
            current = self._version(scope, expected_version, db)
            target_version = current["parent_version"] if target_version is None else target_version
            if type(target_version) is not int or not 1 <= target_version < expected_version:
                raise ContractError("evolution invalid rollback target")
            target = self._version(scope, target_version, db)
            version = expected_version + 1
            payload = {"scope": scope, "version": version, "parent_version": expected_version,
                       "config": target["config"], "candidate_id": target["candidate_id"],
                       "reason": reason, "restored_from_version": target_version}
            db.execute("INSERT INTO r7_evo_versions VALUES(?,?,?,?,?,?,?,?)",
                       (self.owner_id, scope, version, expected_version, dumps(payload), key(payload),
                        target["candidate_id"], self.store.clock()))
            db.execute("UPDATE r7_evo_active SET version=? WHERE owner_id=? AND scope=?",
                       (version, self.owner_id, scope))
            if current["candidate_id"]:
                db.execute("UPDATE r7_evo_candidates SET state='rolled_back' WHERE candidate_id=?",
                           (current["candidate_id"],))
            if target["candidate_id"]:
                db.execute("UPDATE r7_evo_candidates SET state='active' WHERE candidate_id=?", (target["candidate_id"],))
            self.store.audit(db, "r7_configuration_rolled_back", scope, reason)
        return {"state": "configuration_restored", "activated": True, "business_effects_rolled_back": False, **payload}

    def one_cycle(self, journal, **kwargs):
        candidate = None
        scope = kwargs.get("scope")
        # An unbound port can later be connected without requiring another user
        # feedback message or spending a second proposal call.
        pending = self.store.rows("""SELECT candidate_id,state FROM r7_evo_candidates
            WHERE owner_id=? AND scope=? AND state IN ('proposed','replaying','deferred','eligible')
            ORDER BY created,candidate_id LIMIT 200""", (self.owner_id, scope))
        for item in pending:
            if item["state"] == "eligible" and scope not in self.enabled_scopes:
                continue
            row, payload = self._candidate(item["candidate_id"])
            if (payload["runtime_revision"] != self.runtime_revision
                    or payload["development"]["suite_ref"] != kwargs.get("development_ref")
                    or (payload["holdout"] or {}).get("suite_ref") != kwargs.get("holdout_ref")
                    or payload["parent_version"] != self.active_config(scope)["version"]):
                continue
            try:
                self._check_candidate_policy(payload)
            except ContractError:
                # A prior policy's evidence is left intact for audit. Only a
                # freshly bound candidate may proceed under the current gate.
                continue
            if item["state"] == "eligible":
                report_row = self.store.one("SELECT * FROM r7_evo_runs WHERE run_id=?", (row["active_run_id"],))
                report = self._decoded(report_row, payload="report_json", digest="report_hash")
                report["activation"] = self.activate(item["candidate_id"], expected_version=payload["parent_version"])
                return report
            candidate = {"candidate_id": item["candidate_id"], **payload}
            break
        if candidate is None:
            candidate = self.propose_from_journal(journal, **kwargs)
        if not candidate.get("candidate_id"):
            return candidate
        report = self.replay(candidate["candidate_id"])
        if report.get("eligible"):
            report["activation"] = self.activate(candidate["candidate_id"],
                                                  expected_version=candidate["parent_version"])
        return report

    def status(self):
        rows = self.store.rows("SELECT state,COUNT(*) AS n FROM r7_evo_candidates WHERE owner_id=? GROUP BY state",
                               (self.owner_id,))
        scopes = self.store.rows("SELECT scope,version FROM r7_evo_active WHERE owner_id=?", (self.owner_id,))
        proposals = self.store.rows("SELECT state,COUNT(*) AS n FROM r7_evo_proposal_attempts WHERE owner_id=? GROUP BY state",
                                   (self.owner_id,))
        return {"schema_version": VERSION, "candidate_counts": {r["state"]: r["n"] for r in rows},
                "proposal_attempt_counts": {r["state"]: r["n"] for r in proposals},
                "active_scopes": scopes, "enabled_scopes": sorted(self.enabled_scopes),
                "bindings": {"proposer": callable(self.proposer), "replayer": callable(self.replayer),
                             "suite_loader": callable(self.suite_loader)},
                "promotion_policy": self.promotion_policy,
                "relative_quality_scopes": sorted(self.relative_quality_scopes),
                "model_training_performed": False, "business_executor_present": False}

