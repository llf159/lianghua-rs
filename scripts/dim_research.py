#!/usr/bin/env python3
"""相关性研究的复跑与前后对比。

先看用法:
    dim_research.py snapshot <tag>
    dim_research.py compare <before_tag> [after_tag]
    dim_research.py list

snapshot 会对一个 tag 运行:
    - frequency / alphabetical / reverse_frequency 各一次(前 20 规则)
    - 25 次固定随机顺序(排除历史过短的规则), 用于消除顺序偏差
并把每个配置的完整 JSON 存到 <out>/<tag>/ 下, 同时写 manifest.json。

compare 读取两个 tag 的 manifest 与 JSON, 输出逐规则:
    - 全期分数加权收益 mean / HAC t 的前后变化
    - 样本外增量的平均 t / 为正比例 / 取值区间 的前后变化
    - 平均被解释方差(线性正交)的前后变化
以及策略对距离相关的最大变化。

本脚本不触发重算(重算由外部完成); snapshot 记录评分库 mtime,
compare 会直接显示两个 tag 的 mtime, 便于确认 after 确实来自重算后的库。
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_SOURCE = "/run/media/lmingyuanl/mingyuan-512sk/lianghua-data/source"
DEFAULT_BIN = str(Path(__file__).resolve().parent.parent / "target" / "debug" / "strategy_dimension_research")
DEFAULT_OUT = os.path.expanduser("~/.local/share/lianghua-dim-research")
DEFAULT_START = "20260105"
DEFAULT_END = "20260917"
DEFAULT_HOLDING = 5
DEFAULT_NONLINEAR = 512
RULE_COUNT = 20
RANDOM_PERMUTATIONS = 25
RANDOM_SEED = 42
SHORT_HISTORY_RATIO = 0.8


def result_db_path(source: str) -> Path:
    return Path(source) / "scoring_result.db"


def run_one(binary: str, source: str, start: str, end: str, rules, order: str, out_json: Path,
            holding: int, nonlinear: int) -> dict:
    args = [binary, source, start, end, str(RULE_COUNT), str(nonlinear), str(holding), order]
    if rules is not None:
        args.append(",".join(rules))
    env = dict(os.environ)
    env["LIANGHUA_DIM_RESEARCH_JSON"] = str(out_json)
    proc = subprocess.run(args, capture_output=True, text=True, env=env, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"研究失败(order={order}, rules={len(rules) if rules else RULE_COUNT}):\n{proc.stderr.strip()}")
    return json.loads(out_json.read_text(encoding="utf-8"))


def cmd_snapshot(args: argparse.Namespace) -> int:
    out_dir = Path(args.out) / args.tag
    out_dir.mkdir(parents=True, exist_ok=True)
    db = result_db_path(args.source)
    if not db.is_file():
        print(f"评分库不存在:{db}", file=sys.stderr)
        return 1
    db_stat = db.stat()
    manifest = {
        "tag": args.tag,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": args.source,
        "binary": args.bin,
        "start_date": args.start,
        "end_date": args.end,
        "holding_period": args.holding,
        "nonlinear_sample_limit": args.nonlinear,
        "result_db_mtime": db_stat.st_mtime,
        "result_db_size": db_stat.st_size,
        "configs": [],
    }
    print(f"[snapshot {args.tag}] 评分库 mtime={db_stat.st_mtime:.0f} size={db_stat.st_size}")

    def record(name: str, order: str, rules, data: dict):
        manifest["configs"].append({
            "name": name,
            "order": order,
            "rules": rules,
            "universe_sample_count": data["universe_sample_count"],
            "oos_test_start_date": data.get("oos_test_start_date"),
        })
        print(f"  {name}: 规则={len(data['strategies'])} 宇宙={data['universe_sample_count']} "
              f"oos_start={data.get('oos_test_start_date')}")

    freq = run_one(args.bin, args.source, args.start, args.end, None, "frequency",
                   out_dir / "00-frequency.json", args.holding, args.nonlinear)
    record("00-frequency", "frequency", None, freq)

    for idx, order in enumerate(["alphabetical", "reverse_frequency"], start=1):
        data = run_one(args.bin, args.source, args.start, args.end, None, order,
                       out_dir / f"{idx:02d}-{order}.json", args.holding, args.nonlinear)
        record(f"{idx:02d}-{order}", order, None, data)

    max_days = max(s["valid_day_count"] for s in freq["return_summaries"])
    stable_rules = [s["rule_name"] for s in freq["return_summaries"]
                    if s["valid_day_count"] >= SHORT_HISTORY_RATIO * max_days]
    skipped = [s["rule_name"] for s in freq["return_summaries"] if s["rule_name"] not in stable_rules]
    if skipped:
        print(f"  随机顺序排除短历史规则:{skipped}")
    rng = random.Random(RANDOM_SEED)
    for i in range(RANDOM_PERMUTATIONS):
        perm = stable_rules[:]
        rng.shuffle(perm)
        data = run_one(args.bin, args.source, args.start, args.end, perm, "frequency",
                       out_dir / f"rand-{i:02d}.json", args.holding, args.nonlinear)
        record(f"rand-{i:02d}", "explicit", perm, data)

    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"[snapshot {args.tag}] 完成, {len(manifest['configs'])} 个配置 -> {out_dir}")
    return 0


def load_tag(out: str, tag: str):
    root = Path(out) / tag
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    configs = []
    for cfg in manifest["configs"]:
        data = json.loads((root / f"{cfg['name']}.json").read_text(encoding="utf-8"))
        configs.append({"manifest": cfg, "data": data})
    return manifest, configs


def summarize(configs):
    """全期收益取全部配置(顺序无关); 增量/正交/距离相关只用随机顺序配置。

    停用规则会让评分序列恒为 0, 出现在增量前缀里会使后续共同样本为空,
    因此确定性顺序(含被停用规则)预测值不可靠, 统一改用随机顺序配置。
    """
    full = {}
    inc_t = {}
    inc_pos = {}
    inc_mean = {}
    explained = {}
    pair_dcor = {}
    for cfg in configs:
        data = cfg["data"]
        is_random = cfg["manifest"]["name"].startswith("rand-")
        for s in data["return_summaries"]:
            full.setdefault(s["rule_name"], []).append(
                (s["avg_residual_return"], s["hac_t_value"], s["valid_day_count"]))
        if not is_random:
            continue
        for inc in data["return_increments"]:
            t = inc["test_incremental_hac_t_value"]
            m = inc["test_incremental_mean"]
            p = inc["test_incremental_positive_ratio"]
            if t is None or t != t:
                continue
            inc_t.setdefault(inc["rule_name"], []).append(t)
            inc_mean.setdefault(inc["rule_name"], []).append(m)
            inc_pos.setdefault(inc["rule_name"], []).append(p)
        for diag in data["orthogonal_diagnostics"]:
            ev = diag["explained_variance_ratio"]
            if ev is not None and ev == ev:
                explained.setdefault(diag["rule_name"], []).append(ev)
        for p in data["pair_metrics"]:
            key = tuple(sorted((p["left_rule_name"], p["right_rule_name"])))
            d = p["distance_correlation_daily_mean"]
            if d is not None and d == d:
                pair_dcor.setdefault(key, []).append(d)
    return full, inc_t, inc_pos, inc_mean, explained, pair_dcor


def fmt(value, spec=".3f", dash="-"):
    if value is None or value != value:
        return dash
    return format(value, spec)


def cmd_compare(args: argparse.Namespace) -> int:
    bman, bcfg = load_tag(args.out, args.before)
    aman, acfg = load_tag(args.out, args.after)
    print(f"before={args.before}  评分库 mtime={bman['result_db_mtime']:.0f}  created={bman['created_at']}")
    print(f"after ={args.after}  评分库 mtime={aman['result_db_mtime']:.0f}  created={aman['created_at']}")
    if bman["result_db_mtime"] == aman["result_db_mtime"]:
        print("注意:两个快照的评分库 mtime 相同,after 可能不是重算后的库。")
    print()

    bfull, binc, bpos, bmean, bexp, bpair = summarize(bcfg)
    afull, ainc, apos, amean, aexp, apair = summarize(acfg)

    def clean_mean(values):
        clean = [v for v in values if v is not None and v == v]
        return statistics.mean(clean) if clean else None

    def full_t(n):
        value = clean_mean(x[1] for x in (afull.get(n) or bfull.get(n, [])))
        return value if value is not None else float("-inf")

    names = sorted(set(bfull) | set(afull), key=lambda n: -full_t(n))

    print("=== 全期分数加权收益(与顺序无关) ===")
    print(f"{'规则':<22}{'before t':>10}{'after t':>10}{'delta':>9}{'after days':>11}")
    for n in names:
        bt = clean_mean(x[1] for x in bfull[n]) if n in bfull else None
        at = clean_mean(x[1] for x in afull[n]) if n in afull else None
        days = afull[n][0][2] if n in afull else (bfull[n][0][2] if n in bfull else None)
        delta = (at - bt) if (bt is not None and at is not None) else None
        print(f"{n:<22}{fmt(bt):>10}{fmt(at):>10}{fmt(delta):>9}{str(days):>11}")

    print("\n=== 样本外增量(跨顺序平均) ===")
    print(f"{'规则':<22}{'before t':>10}{'after t':>10}{'delta':>9}{'b%>0':>7}{'a%>0':>7}{'after n':>8}")
    for n in names:
        bt = statistics.mean(binc[n]) if n in binc else None
        at = statistics.mean(ainc[n]) if n in ainc else None
        bp = 100 * statistics.mean(bpos[n]) if n in bpos else None
        ap = 100 * statistics.mean(apos[n]) if n in apos else None
        n_after = len(ainc.get(n, []))
        delta = (at - bt) if (bt is not None and at is not None) else None
        print(f"{n:<22}{fmt(bt):>10}{fmt(at):>10}{fmt(delta):>9}{fmt(bp,'.0f'):>7}{fmt(ap,'.0f'):>7}{n_after:>8}")

    print("\n=== 线性正交被解释方差(跨顺序平均) ===")
    print(f"{'规则':<22}{'before':>10}{'after':>10}{'delta':>9}")
    for n in names:
        be = statistics.mean(bexp[n]) if n in bexp else None
        ae = statistics.mean(aexp[n]) if n in aexp else None
        delta = (ae - be) if (be is not None and ae is not None) else None
        print(f"{n:<22}{fmt(be):>10}{fmt(ae):>10}{fmt(delta):>9}")

    print("\n=== 距离相关变化最大的策略对 ===")
    diffs = []
    for key in set(bpair) | set(apair):
        b = statistics.mean(bpair[key]) if key in bpair else None
        a = statistics.mean(apair[key]) if key in apair else None
        if b is None or a is None:
            continue
        diffs.append((abs(a - b), key, b, a))
    for _, (l, r), b, a in sorted(diffs, reverse=True)[:10]:
        print(f"  {l} / {r}: dCor {b:.3f} -> {a:.3f} ({a - b:+.3f})")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    root = Path(args.out)
    if not root.is_dir():
        print(f"输出目录不存在:{root}")
        return 0
    for tag in sorted(p.name for p in root.iterdir() if (p / "manifest.json").is_file()):
        m = json.loads((root / tag / "manifest.json").read_text(encoding="utf-8"))
        print(f"{tag:<24} created={m['created_at']}  db_mtime={m['result_db_mtime']:.0f}  configs={len(m['configs'])}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("snapshot", help="运行一组配置并保存 JSON 快照")
    p.add_argument("tag")
    p.add_argument("--source", default=DEFAULT_SOURCE)
    p.add_argument("--bin", default=DEFAULT_BIN)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--start", default=DEFAULT_START)
    p.add_argument("--end", default=DEFAULT_END)
    p.add_argument("--holding", type=int, default=DEFAULT_HOLDING)
    p.add_argument("--nonlinear", type=int, default=DEFAULT_NONLINEAR)
    p.set_defaults(func=cmd_snapshot)

    p = sub.add_parser("compare", help="对比两个快照")
    p.add_argument("before")
    p.add_argument("after", nargs="?")
    p.add_argument("--out", default=DEFAULT_OUT)
    p.set_defaults(func=lambda a: cmd_compare(a))

    p = sub.add_parser("list", help="列出所有快照")
    p.add_argument("--out", default=DEFAULT_OUT)
    p.set_defaults(func=cmd_list)

    args = parser.parse_args()
    if args.command == "compare" and args.after is None:
        root = Path(args.out)
        tags = sorted(p.name for p in root.iterdir() if (p / "manifest.json").is_file()) if root.is_dir() else []
        if len(tags) < 2:
            print("至少需要两个快照才能对比", file=sys.stderr)
            return 1
        args.before, args.after = tags[-2], tags[-1]
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
