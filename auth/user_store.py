import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from werkzeug.security import check_password_hash, generate_password_hash


@dataclass
class UserRecord:
    username: str
    email: str
    password_hash: str
    roles: List[str] = field(default_factory=list)
    flags: Dict[str, object] = field(default_factory=dict)
    approved: bool = True

    @classmethod
    def from_dict(cls, raw: Dict[str, object]) -> "UserRecord":
        return cls(
            username=str(raw.get("username", "")).strip(),
            email=str(raw.get("email", "")).strip(),
            password_hash=str(raw.get("password_hash", "")),
            roles=list(raw.get("roles") or []),
            flags=dict(raw.get("flags") or {}),
            approved=bool(raw.get("approved", True)),
        )

    def to_dict(self) -> Dict[str, object]:
        return {
            "username": self.username,
            "email": self.email,
            "password_hash": self.password_hash,
            "roles": self.roles,
            "flags": self.flags,
            "approved": self.approved,
        }


class UserStore:
    def __init__(self, storage_path: Path):
        self.storage_path = storage_path
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> List[UserRecord]:
        if not self.storage_path.exists():
            return []
        try:
            raw_data = json.loads(self.storage_path.read_text(encoding="utf-8"))
            return [UserRecord.from_dict(item) for item in raw_data if isinstance(item, dict)]
        except (OSError, json.JSONDecodeError):
            return []

    def _save(self, users: List[UserRecord]) -> None:
        serialized = [user.to_dict() for user in users]
        self.storage_path.write_text(json.dumps(serialized, indent=2), encoding="utf-8")

    def get_user(self, username: str) -> Optional[UserRecord]:
        username = username.lower().strip()
        if not username:
            return None
        for user in self._load():
            if user.username.lower() == username:
                return user
        return None

    def email_exists(self, email: str) -> bool:
        target = email.lower().strip()
        if not target:
            return False
        return any(user.email.lower() == target for user in self._load())

    def get_user_by_email(self, email: str) -> Optional[UserRecord]:
        target = email.lower().strip()
        if not target:
            return None
        for user in self._load():
            if user.email.lower() == target:
                return user
        return None

    def create_user(
        self,
        username: str,
        email: str,
        password: str,
        *,
        roles: Optional[List[str]] = None,
        flags: Optional[Dict[str, object]] = None,
        approved: bool = False,
    ) -> UserRecord:
        user = UserRecord(
            username=username.strip(),
            email=email.strip(),
            password_hash=generate_password_hash(password),
            roles=roles or [],
            flags=flags or {},
            approved=approved,
        )
        users = [u for u in self._load() if u.username.lower() != user.username.lower()]
        users.append(user)
        self._save(users)
        return user

    def authenticate(self, username: str, password: str) -> Optional[UserRecord]:
        user = self.get_user(username)
        if not user:
            return None
        if not check_password_hash(user.password_hash, password):
            return None
        return user

    def update_password(self, username: str, password: str) -> Optional[UserRecord]:
        updated = None
        users = []
        for user in self._load():
            if user.username.lower() == username.lower():
                user.password_hash = generate_password_hash(password)
                flags = dict(user.flags or {})
                flags.pop("reset_token_hash", None)
                flags.pop("reset_token_expires_at", None)
                flags.pop("reset_requested_at", None)
                user.flags = flags
                updated = user
            users.append(user)
        if updated:
            self._save(users)
        return updated

    def set_reset_token(self, username: str, token_hash: str, expires_at: datetime) -> Optional[UserRecord]:
        updated = None
        expires_at_utc = expires_at.astimezone(timezone.utc)
        users = []
        for user in self._load():
            if user.username.lower() == username.lower():
                flags = dict(user.flags or {})
                flags["reset_token_hash"] = token_hash
                flags["reset_token_expires_at"] = expires_at_utc.isoformat()
                flags["reset_requested_at"] = datetime.now(timezone.utc).isoformat()
                user.flags = flags
                updated = user
            users.append(user)
        if updated:
            self._save(users)
        return updated

    def get_user_by_reset_token(self, token_hash: str) -> Optional[UserRecord]:
        if not token_hash:
            return None
        for user in self._load():
            flags = dict(user.flags or {})
            if flags.get("reset_token_hash") == token_hash:
                return user
        return None

    def clear_reset_token(self, username: str) -> Optional[UserRecord]:
        updated = None
        users = []
        for user in self._load():
            if user.username.lower() == username.lower():
                flags = dict(user.flags or {})
                flags.pop("reset_token_hash", None)
                flags.pop("reset_token_expires_at", None)
                flags.pop("reset_requested_at", None)
                user.flags = flags
                updated = user
            users.append(user)
        if updated:
            self._save(users)
        return updated

    def approve_user(self, username: str, approved: bool = True) -> Optional[UserRecord]:
        updated = None
        users = []
        for user in self._load():
            if user.username.lower() == username.lower():
                user.approved = approved
                updated = user
            users.append(user)
        if updated:
            self._save(users)
        return updated

    def update_roles(self, username: str, roles: List[str]) -> Optional[UserRecord]:
        updated = None
        users = []
        normalized_roles = [r.strip() for r in roles if r.strip()]
        for user in self._load():
            if user.username.lower() == username.lower():
                user.roles = normalized_roles
                updated = user
            users.append(user)
        if updated:
            self._save(users)
        return updated

    def ensure_seed_users(self, seeds: Dict[str, Dict[str, object]]) -> None:
        users = self._load()
        existing_usernames = {u.username.lower() for u in users}
        changed = False
        for username, meta in seeds.items():
            if username.lower() in existing_usernames:
                continue
            password = str(meta.get("password", "")).strip()
            email = str(meta.get("email", f"{username}@example.com"))
            roles = list(meta.get("roles") or [])
            approved = bool(meta.get("approved", True))
            users.append(
                UserRecord(
                    username=username,
                    email=email,
                    password_hash=generate_password_hash(password) if password else "",
                    roles=roles,
                    approved=approved,
                    flags=dict(meta.get("flags") or {}),
                )
            )
            changed = True
        if changed:
            self._save(users)

    def list_users(self) -> List[UserRecord]:
        return sorted(self._load(), key=lambda u: u.username.lower())

