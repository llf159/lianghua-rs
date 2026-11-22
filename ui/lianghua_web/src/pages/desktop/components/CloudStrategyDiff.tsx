import { useMemo, useState } from "react";
import { formatStrategyExpression } from "../../../shared/strategyExpression";
import type { CloudPreviewFile } from "../../../apis/cloudSync";

type Field = { label: string; value: string };

function collectFields(
  value: unknown,
  path: string[],
  fields: Map<string, Field>,
) {
  if (
    Array.isArray(value) &&
    value.some((item) => item && typeof item === "object")
  ) {
    const occurrences = new Map<string, number>();
    const identities: string[] = [];
    value.forEach((item, index) => {
      const identity = String(item?.name ?? item?.id ?? `第 ${index + 1} 项`);
      const occurrence = (occurrences.get(identity) ?? 0) + 1;
      occurrences.set(identity, occurrence);
      const name =
        occurrence > 1 ? `${identity}（第 ${occurrence} 项）` : identity;
      identities.push(name);
      collectFields(item, [...path, name], fields);
    });
    fields.set(JSON.stringify([...path, "$order"]), {
      label: [...path, "排列顺序"].join(" / "),
      value: identities.join("\n"),
    });
  } else if (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value)
  ) {
    for (const [key, child] of Object.entries(value))
      collectFields(child, [...path, key], fields);
  } else {
    fields.set(JSON.stringify(path), {
      label: path.join(" / "),
      value: typeof value === "string" ? value : JSON.stringify(value),
    });
  }
}

export default function CloudStrategyDiff({
  local,
  remote,
  remoteLabel,
}: {
  local?: CloudPreviewFile;
  remote?: CloudPreviewFile;
  remoteLabel: string;
}) {
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState("");
  const fields = useMemo(() => {
    const left = new Map<string, Field>();
    const right = new Map<string, Field>();
    if (local?.structured != null) collectFields(local.structured, [], left);
    if (remote?.structured != null) collectFields(remote.structured, [], right);
    return [...new Set([...left.keys(), ...right.keys()])].flatMap((key) => {
      const before = left.get(key);
      const after = right.get(key);
      return before?.value === after?.value
        ? []
        : [
            {
              key,
              label: (before ?? after)!.label,
              before: before?.value,
              after: after?.value,
            },
          ];
    });
  }, [local, remote]);
  const visible = fields.filter((field) =>
    field.label.toLowerCase().includes(filter.toLowerCase()),
  );
  const active = visible.find((field) => field.key === selected) ?? visible[0];
  return (
    <div className="cloud-strategy-diff">
      <div className="cloud-review-toolbar">
        <strong>{fields.length} 个字段有变化</strong>
        <input
          aria-label="搜索变动规则"
          placeholder="搜索规则名或字段名"
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
        />
      </div>
      <p className="cloud-watch-hint">
        只列出发生变化的规则和字段；选择一项查看两端内容。排版和注释变化请查看原文。
      </p>
      <div className="cloud-strategy-changes" aria-label="变动字段">
        {visible.map((field) => (
          <button
            type="button"
            key={field.key}
            className={active?.key === field.key ? "is-selected" : ""}
            onClick={() => setSelected(field.key)}
          >
            <span>
              {field.before === undefined
                ? "仅云端有"
                : field.after === undefined
                  ? "仅本地有"
                  : "修改"}
            </span>
            <strong>
              {field.label
                .replace(/^rule \/ /, "规则 / ")
                .replace(/^scene \/ /, "场景 / ")
                .replace(/^track \/ /, "跟踪 / ")}
            </strong>
          </button>
        ))}
        {!visible.length && (
          <p>
            {fields.length
              ? "没有匹配的变动"
              : local?.hash === remote?.hash
                ? "两端策略完全相同。"
                : "策略配置相同，文件差异来自排版或注释。"}
          </p>
        )}
      </div>
      {active && (
        <div className="cloud-strategy-field" key={active.key}>
          <strong>{active.label}</strong>
          <div className="cloud-field-versions">
            {[
              { label: "当前本地", text: active.before },
              { label: remoteLabel, text: active.after },
            ].map((side, index) => {
              let prefix = 0;
              let suffix = 0;
              const text =
                side.text !== undefined &&
                /(?:when|expression|formula)$/.test(active.label)
                  ? formatStrategyExpression(side.text, 60)
                  : side.text;
              const counterpart = index ? active.before : active.after;
              const other =
                counterpart !== undefined &&
                /(?:when|expression|formula)$/.test(active.label)
                  ? formatStrategyExpression(counterpart, 60)
                  : counterpart;
              if (text !== undefined && other !== undefined) {
                while (
                  prefix < text.length &&
                  prefix < other.length &&
                  text[prefix] === other[prefix]
                )
                  prefix++;
                while (
                  suffix < text.length - prefix &&
                  suffix < other.length - prefix &&
                  text[text.length - suffix - 1] ===
                    other[other.length - suffix - 1]
                )
                  suffix++;
              }
              return (
                <section key={side.label}>
                  <h5>{side.label}</h5>
                  <pre>
                    {text === undefined ? (
                      "此项不存在"
                    ) : (
                      <>
                        {text.slice(0, prefix)}
                        <mark>
                          {text.slice(prefix, text.length - suffix) || " "}
                        </mark>
                        {suffix ? text.slice(-suffix) : ""}
                      </>
                    )}
                  </pre>
                </section>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
