"""Lightweight FAQ-style chatbot for the AccSafety portal user guide."""

from __future__ import annotations

from difflib import SequenceMatcher
from typing import Dict, List, Sequence


class GuideChatbot:
    def __init__(self) -> None:
        self.topics: List[Dict[str, object]] = [
            {
                "title": "Signing in and sessions",
                "question": "How do I log in to the AccSafety portal?",
                "answer": (
                    "Open the secure login page, sign in with your assigned credentials, and accept the data use "
                    "acknowledgement. If you close the browser, log out from the header to end the session."
                ),
                "keywords": ["login", "log in", "session", "timeout", "logout", "consent", "sign in"],
                "links": [
                    {"label": "Go to login", "href": "/login"},
                    {"label": "Portal home", "href": "/"},
                ],
            },
            {
                "title": "Using the dataset explorer",
                "question": "How do I explore statewide datasets?",
                "answer": (
                    "Select the Explore Available Datasets option to open the unified explorer. Apply the Mode, Facility, and "
                    "Data Source filters across the top to enable the map and location list, then follow the view links for "
                    "trends."
                ),
                "keywords": ["dataset", "explore", "filters", "mode", "facility", "data source", "map"],
                "links": [
                    {"label": "Launch explorer", "href": "/explore/"},
                ],
            },
            {
                "title": "Live counts and detections",
                "question": "Where can I see live counts?",
                "answer": (
                    "Use the Live Counts app for computer-vision safety insights or the Live Detection Monitor to view "
                    "real-time multimodal activity. Both links are available from the portal home."
                ),
                "keywords": ["live counts", "detection", "real time", "monitor", "vivacity", "multimodal"],
                "links": [
                    {"label": "Open Live Counts", "href": "/vivacity/"},
                    {"label": "Open Live Detection", "href": "/live/"},
                ],
            },
            {
                "title": "Progressive filters",
                "question": "What do the progressive filters mean?",
                "answer": (
                    "Mode limits the data to pedestrians, cyclists, or combined counts. Facility switches between facility types, "
                    "and Data Source narrows to the selected provider. All three need a choice before maps and locations appear."
                ),
                "keywords": ["progressive filters", "mode", "facility", "data source", "counts", "map"],
                "links": [
                    {"label": "Review filter tips", "href": "#dataset-explorer"},
                ],
            },
            {
                "title": "Support",
                "question": "Who can help me with account or data questions?",
                "answer": (
                    "Email the UWM IPIT team for help with account provisioning, password resets, or onboarding materials. "
                    "Self-service resets are not yet available."
                ),
                "keywords": ["support", "help", "contact", "account", "password", "onboarding"],
                "links": [],
            },
        ]

    def _similarity(self, user_message: str, topic: Dict[str, object]) -> float:
        combined_text_parts: List[str] = [
            str(topic.get("question", "")),
            " ".join(topic.get("keywords", [])),
            str(topic.get("title", "")),
        ]
        combined_text = " ".join(part.lower() for part in combined_text_parts if part)
        base_score = SequenceMatcher(None, user_message.lower(), combined_text).ratio()

        keywords: Sequence[str] = topic.get("keywords", [])  # type: ignore[assignment]
        keyword_hits = sum(1 for word in keywords if word.lower() in user_message.lower())
        bonus = min(keyword_hits * 0.08, 0.2)
        return min(base_score + bonus, 1.0)

    def answer(self, message: str) -> Dict[str, object]:
        cleaned = message.strip()
        if not cleaned:
            return {
                "response": "Ask me about logging in, exploring datasets, or where to find live counts.",
                "topic": None,
                "confidence": 0.0,
                "suggested_topics": [topic["title"] for topic in self.topics],
                "links": [],
            }

        scored = [
            (self._similarity(cleaned, topic), topic)
            for topic in self.topics
        ]
        scored.sort(key=lambda item: item[0], reverse=True)

        best_score, best_topic = scored[0]
        fallback = best_score < 0.35

        response_text = best_topic["answer"] if not fallback else (
            "I could not match that to the portal guide. Try asking about logging in, datasets, live counts, or support."
        )

        return {
            "response": response_text,
            "topic": best_topic["title"] if not fallback else None,
            "confidence": round(float(best_score), 3),
            "suggested_topics": [topic["title"] for _, topic in scored[:3]],
            "links": best_topic.get("links", []) if not fallback else [],
        }
