#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from chatbot.policy import evaluate_user_request
from chatbot.retrieval import RetrievalResult
from chatbot.service import ChatService


def _load_cases(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("Fixture must be a JSON list of evaluation cases")
    return data


def _validate_retrieval_prompt(case: dict, service: ChatService) -> list[str]:
    errors: list[str] = []
    intent = service._classify_intent(case.get("message", ""))
    expected_intent = case.get("expected_intent")
    if expected_intent and intent != expected_intent:
        errors.append(f"intent mismatch (expected={expected_intent}, got={intent})")

    retrieval_data = case.get("retrieval") or {}
    retrieval = RetrievalResult(
        evidence=retrieval_data.get("evidence") or [],
        citations=retrieval_data.get("citations") or [],
        stats=retrieval_data.get("stats") or {"by_source": [], "by_facility": [], "by_mode": []},
    )

    if case.get("expect_citations") and not retrieval.citations:
        errors.append("expected citations but fixture has none")

    if not retrieval.evidence:
        errors.append("expected non-empty retrieval evidence")
        return errors

    prompt = service._build_constraint_prompt(retrieval, intent)

    for fact in case.get("expected_key_facts") or []:
        if fact not in prompt:
            errors.append(f"missing expected key fact in prompt: {fact}")

    first = retrieval.evidence[0]
    expected_line = rf"1\. \[{re.escape(first['source'])}\] {re.escape(first['title'])}:"
    if not re.search(expected_line, prompt):
        errors.append("evidence formatting mismatch for first prompt line")

    return errors


def _validate_refusal(case: dict) -> list[str]:
    errors: list[str] = []
    decision = evaluate_user_request(message=case.get("message", ""), history=None)
    if decision.allowed:
        errors.append("expected refusal but request was allowed")

    expected_reason = case.get("expected_refusal_reason")
    if expected_reason and decision.reason != expected_reason:
        errors.append(f"refusal reason mismatch (expected={expected_reason}, got={decision.reason})")
    return errors


def run_eval(cases: list[dict]) -> int:
    service = ChatService(provider=None, retriever=None)
    failures = 0

    for case in cases:
        case_id = case.get("id", "<unknown>")
        if case.get("expected_refusal"):
            errors = _validate_refusal(case)
        else:
            errors = _validate_retrieval_prompt(case, service)

        if errors:
            failures += 1
            print(f"[FAIL] {case_id}")
            for err in errors:
                print(f"  - {err}")
        else:
            print(f"[PASS] {case_id}")

    print(f"\nEvaluated {len(cases)} cases; failures={failures}")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run deterministic chatbot prompt/retrieval checks")
    parser.add_argument(
        "--cases",
        type=Path,
        default=Path("tests/fixtures/chat_eval_cases.json"),
        help="Path to chat evaluation fixture JSON",
    )
    args = parser.parse_args()

    try:
        cases = _load_cases(args.cases)
    except Exception as exc:
        print(f"Failed to load cases: {exc}", file=sys.stderr)
        return 2

    return run_eval(cases)


if __name__ == "__main__":
    raise SystemExit(main())
