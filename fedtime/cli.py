"""命令行入口：启动服务、重放演练、按同一运行口径查询。"""

from __future__ import annotations

import argparse
import json
import sys

from .engine import Coordinator, ProtocolError
from .runner import replay_directory, replay_file
from .server import serve
from .storage import Storage


def _print(payload):
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="fedtime", description="联邦逻辑时间协调服务（本地合成仿真）")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_serve = sub.add_parser("serve", help="启动本地 HTTP 协调服务")
    p_serve.add_argument("--db", required=True)
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8765)

    p_replay = sub.add_parser("replay", help="重放 JSONL 演练资料")
    p_replay.add_argument("path", help="单个 .jsonl 文件或目录")
    p_replay.add_argument("--db", default=":memory:")
    p_replay.add_argument("--transcript", action="store_true",
                          help="打印逐条输入与处理结果")

    p_inspect = sub.add_parser("inspect", help="查询授予/应用/等待视图")
    p_inspect.add_argument("--db", required=True)
    p_inspect.add_argument("--run", required=True)
    p_inspect.add_argument("--endpoint")

    p_dead = sub.add_parser("deadlocks", help="查询相互等待环")
    p_dead.add_argument("--db", required=True)
    p_dead.add_argument("--run", required=True)

    p_log = sub.add_parser("log", help="打印只追加的审计日志")
    p_log.add_argument("--db", required=True)

    args = parser.parse_args(argv)

    if args.cmd == "serve":
        httpd, _ = serve(args.db, args.host, args.port)
        print(f"联邦时间协调服务监听 http://{args.host}:{args.port} "
              f"（持久库 {args.db}）", file=sys.stderr)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            httpd.shutdown()
        return 0

    if args.cmd == "replay":
        from pathlib import Path
        path = Path(args.path)
        if path.is_dir():
            runs = replay_directory(path, args.db)
            summary = {}
            for name, bundle in runs.items():
                summary[name] = {
                    "messages": len(bundle["transcript"]),
                    "grants": len(bundle["coordinator"].grants),
                    "endpoints": len(bundle["coordinator"].endpoints)}
                if args.transcript:
                    _print({"file": name,
                            "transcript": bundle["transcript"]})
            if not args.transcript:
                _print(summary)
        else:
            store = Storage(args.db)
            coordinator = Coordinator(store)
            transcript = replay_file(path, coordinator)
            if args.transcript:
                _print(transcript)
            else:
                _print({"messages": len(transcript),
                        "grants": len(coordinator.grants),
                        "endpoints": len(coordinator.endpoints)})
        return 0

    store = Storage(args.db)
    coordinator = Coordinator(store)
    try:
        if args.cmd == "inspect":
            record = {"run": args.run}
            if args.endpoint:
                record["endpoint"] = args.endpoint
            _print(coordinator.process(0, "inspect", record))
        elif args.cmd == "deadlocks":
            _print(coordinator.find_deadlocks(args.run))
        elif args.cmd == "log":
            _print(coordinator.db.read_log())
    except ProtocolError as error:
        print(f"协议错误：{error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
