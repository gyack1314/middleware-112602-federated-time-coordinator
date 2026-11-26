"""把演练资料（JSONL）按到达顺序重放到协调服务。

复用 scenario_reader 的封装校验，保证服务输入与本地合成契约一致。
"""

from __future__ import annotations

import json
from pathlib import Path

from scenario_reader import iter_messages

from .engine import Coordinator
from .storage import Storage

MESSAGE_TYPES = [
    "advance", "applied", "edge", "event",
    "inspect", "join", "promise", "restart",
]


def replay_file(path, coordinator: Coordinator, supported=None):
    """重放单个 JSONL 文件，返回逐条 (消息, 结果) 转录。"""
    known = set(supported or MESSAGE_TYPES)
    transcript = []
    for row in iter_messages(Path(path), known):
        result = coordinator.process(row["at"], row["kind"], row["record"])
        transcript.append({"message": row, "result": result})
    return transcript


def replay_directory(directory, db_path=":memory:"):
    """重放目录内全部场景，每个文件使用独立的内存/文件协调服务。"""
    directory = Path(directory)
    known = set(json.loads(
        (directory / "message_types.json").read_text(encoding="utf-8")))
    runs = {}
    for path in sorted(directory.glob("*.jsonl")):
        store = Storage(db_path if db_path == ":memory:"
                        else f"{db_path}.{path.stem}.db")
        coordinator = Coordinator(store)
        transcript = replay_file(path, coordinator, known)
        runs[path.name] = {"coordinator": coordinator,
                           "transcript": transcript}
    return runs
