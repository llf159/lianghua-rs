import { useState } from 'react'
import type { TrackDefinition } from '../../apis/strategyTracking'
import './css/StrategyRuleTrackingEditor.css'

function TrackingPointsInput({ value = 0, onChange }: { value?: number; onChange: (value: number) => void }) {
  const [input, setInput] = useState({ value, text: String(value) })
  // Keep partial numeric input while typing; synchronize when steps are reordered.
  if (!Object.is(input.value, value)) {
    setInput({ value, text: String(value) })
  }
  return (
    <input
      type="number"
      step="any"
      inputMode="decimal"
      value={input.text}
      aria-invalid={!Number.isFinite(value)}
      onChange={(event) => {
        const text = event.target.value
        const value = text.trim() ? Number(text) : NaN
        setInput({ value, text })
        onChange(value)
      }}
    />
  )
}

export default function StrategyRuleTrackingEditor({
  ruleName,
  tracking,
  onChange,
  captureTexts,
  onCaptureTextsChange,
}: {
  ruleName: string
  tracking: TrackDefinition[]
  onChange: (value: TrackDefinition[]) => void
  captureTexts: Record<string, string>
  onCaptureTextsChange: (value: Record<string, string>) => void
}) {
  function updateTrack(index: number, changes: Partial<TrackDefinition>) {
    onChange(tracking.map((item, i) => (i === index ? { ...item, ...changes } : item)))
  }

  function moveStep(index: number, from: number, to: number) {
    const steps = [...(tracking[index].steps ?? [])]
    steps.splice(to, 0, ...steps.splice(from, 1))
    updateTrack(index, { steps })
  }

  return (
    <section className="strategy-rule-tracking strategy-rule-editor-section">
      <div className="strategy-rule-editor-section-head">
        <div>
          <h4>后续跟踪</h4>
        </div>
        <label className="strategy-rule-tracking-toggle">
          <input
            type="checkbox"
            role="switch"
            aria-label="开启后续跟踪"
            checked={tracking.some((item) => item.enabled)}
            onChange={(event) => {
              const enabled = event.target.checked
              onChange(
                tracking.length
                  ? tracking.map((item) => ({ ...item, enabled }))
                  : [
                      {
                        id: crypto.randomUUID(),
                        name: `${ruleName.trim() || '本策略'}后续跟踪`,
                        enabled,
                        source: 'self',
                        ttl: 20,
                        steps: [],
                      },
                    ],
              )
            }}
          />
          <span aria-hidden="true" className="strategy-rule-tracking-switch" />
          <span>{tracking.some((item) => item.enabled) ? '已开启' : '未开启'}</span>
        </label>
      </div>
      {tracking.map(
        (item, index) =>
          (item.enabled || tracking.length > 1) && (
            <div key={item.id} className="strategy-rule-tracking-settings">
              {tracking.length > 1 && (
                <label className="strategy-tracking-check">
                  <input
                    type="checkbox"
                    checked={item.enabled}
                    onChange={(event) => updateTrack(index, { enabled: event.target.checked })}
                  />
                  {item.name}
                </label>
              )}
              {item.enabled && (
                <>
                  <p className="strategy-manage-note">
                    默认在原策略贡献大于 0 时建立起点；填写自定义起点后按该条件建立，原策略填 0 分也可继续跟踪。重复发现不换起点。步骤分和确认分各取得一次，之后持续保留；全部填 0 时只观察。
                  </p>
                  <label className="strategy-manage-field">
                    <span>跟踪名称（在明细与回测中独立展示）</span>
                    <input
                      value={item.name}
                      onChange={(event) => updateTrack(index, { name: event.target.value })}
                    />
                  </label>
                  <label className="strategy-manage-field">
                    <span>自定义起点（留空沿用原策略正贡献）</span>
                    <textarea
                      rows={3}
                      spellCheck={false}
                      autoCapitalize="off"
                      autoCorrect="off"
                      value={item.start ?? ''}
                      onChange={(event) => updateTrack(index, { start: event.target.value })}
                    />
                  </label>
                  <div className="strategy-rule-tracking-duration">
                    <label className="strategy-manage-field">
                      <span>观察期限</span>
                      <div>
                        <input
                          type="number"
                          min="1"
                          step="1"
                          inputMode="numeric"
                          value={item.ttl || ''}
                          onChange={(event) =>
                            updateTrack(index, { ttl: Number(event.target.value) })
                          }
                        />
                        <span>个市场交易日</span>
                      </div>
                    </label>
                  </div>
                  <details className="strategy-rule-tracking-detail">
                    <summary>
                      <strong>冻结起点数据</strong>
                      <span>
                        {captureTexts[item.id] !== undefined
                          ? `${captureTexts[item.id].split('\n').filter((line) => line.trim()).length} 行`
                          : Object.keys(item.capture ?? {}).length
                            ? `${Object.keys(item.capture ?? {}).length} 个变量`
                            : '可选'}
                      </span>
                    </summary>
                    <div className="strategy-rule-tracking-detail-body">
                      <label className="strategy-manage-field">
                        <span>A_变量 = 表达式（每行一个）</span>
                        <textarea
                          rows={4}
                          spellCheck={false}
                          autoCapitalize="off"
                          autoCorrect="off"
                          value={
                            captureTexts[item.id] ??
                            Object.entries(item.capture ?? {})
                              .map(([key, expression]) => `${key} = ${expression}`)
                              .join('\n')
                          }
                          onChange={(event) =>
                            onCaptureTextsChange({ ...captureTexts, [item.id]: event.target.value })
                          }
                          placeholder={'A_LOW = L\nA_CLOSE = C\nA_VOL = V'}
                        />
                      </label>
                    </div>
                  </details>
                  <details className="strategy-rule-tracking-detail">
                    <summary>
                      <strong>失效与当日资格</strong>
                      <span>
                        {[item.invalidate, item.eligible].filter((value) => value?.trim()).length
                          ? `已设 ${[item.invalidate, item.eligible].filter((value) => value?.trim()).length} 项`
                          : '可选'}
                      </span>
                    </summary>
                    <div className="strategy-rule-tracking-detail-body strategy-rule-tracking-condition-grid">
                      {(
                        [
                          [
                            'invalidate',
                            '永久失效条件',
                            '例如 C < A_LOW',
                          ],
                          [
                            'eligible',
                            '当日资格条件',
                            '例如 AGE >= 2',
                          ],
                        ] as const
                      ).map(([key, label, placeholder]) => (
                        <label key={key} className="strategy-manage-field">
                          <span>{label}</span>
                          <textarea
                            rows={3}
                            spellCheck={false}
                            autoCapitalize="off"
                            autoCorrect="off"
                            value={item[key] ?? ''}
                            placeholder={placeholder}
                            onChange={(event) => updateTrack(index, { [key]: event.target.value })}
                          />
                        </label>
                      ))}
                    </div>
                  </details>
                  <details className="strategy-rule-tracking-detail">
                    <summary>
                      <strong>有序观察步骤</strong>
                      <span>
                        {item.steps?.length ? `${item.steps.length} 步` : '无步骤，直接判断信号'}
                      </span>
                    </summary>
                    <div className="strategy-rule-tracking-detail-body">
                      {(item.steps ?? []).map((step, stepIndex) => (
                        <div key={stepIndex} className="strategy-tracking-step">
                          <div className="strategy-rule-tracking-step-head">
                            <strong>步骤 {stepIndex + 1}</strong>
                            <div>
                              <button
                                className="strategy-manage-inline-btn"
                                type="button"
                                disabled={stepIndex === 0}
                                aria-label={`上移步骤 ${stepIndex + 1}`}
                                onClick={() => moveStep(index, stepIndex, stepIndex - 1)}
                              >
                                上移
                              </button>
                              <button
                                className="strategy-manage-inline-btn"
                                type="button"
                                disabled={stepIndex === (item.steps?.length ?? 0) - 1}
                                aria-label={`下移步骤 ${stepIndex + 1}`}
                                onClick={() => moveStep(index, stepIndex, stepIndex + 1)}
                              >
                                下移
                              </button>
                              <button
                                className="strategy-manage-inline-btn is-danger"
                                type="button"
                                aria-label={`删除步骤 ${stepIndex + 1}`}
                                onClick={() =>
                                  updateTrack(index, {
                                    steps: item.steps?.filter((_, j) => j !== stepIndex),
                                  })
                                }
                              >
                                删除
                              </button>
                            </div>
                          </div>
                          <label className="strategy-manage-field">
                            <span>步骤名称</span>
                            <input
                              value={step.name}
                              placeholder="例如：完成缩量观察"
                              onChange={(event) =>
                                updateTrack(index, {
                                  steps: item.steps?.map((entry, j) =>
                                    j === stepIndex
                                      ? { ...entry, name: event.target.value }
                                      : entry,
                                  ),
                                })
                              }
                            />
                          </label>
                          <label className="strategy-manage-field">
                            <span>步骤得分</span>
                            <TrackingPointsInput
                              value={step.points ?? 0}
                              onChange={(points) =>
                                updateTrack(index, {
                                  steps: item.steps?.map((entry, j) =>
                                    j === stepIndex ? { ...entry, points } : entry,
                                  ),
                                })
                              }
                            />
                          </label>
                          <label className="strategy-manage-field">
                            <span>完成条件</span>
                            <textarea
                              rows={3}
                              spellCheck={false}
                              autoCapitalize="off"
                              autoCorrect="off"
                              value={step.when}
                              onChange={(event) =>
                                updateTrack(index, {
                                  steps: item.steps?.map((entry, j) =>
                                    j === stepIndex
                                      ? { ...entry, when: event.target.value }
                                      : entry,
                                  ),
                                })
                              }
                              placeholder="例如 V < A_VOL * 0.8"
                            />
                          </label>
                        </div>
                      ))}
                      <button
                        className="strategy-manage-inline-btn strategy-rule-tracking-add"
                        type="button"
                        onClick={() =>
                          updateTrack(index, {
                            steps: [...(item.steps ?? []), { name: '', when: '' }],
                          })
                        }
                      >
                        ＋ 增加观察步骤
                      </button>
                    </div>
                  </details>
                  <details className="strategy-rule-tracking-detail">
                    <summary>
                      <strong>满足条件与确认分</strong>
                      <span>{item.signal?.trim() ? '已设置条件' : '默认符合资格即成立'}</span>
                    </summary>
                    <div className="strategy-rule-tracking-detail-body">
                      <label className="strategy-manage-field">
                        <span>最终满足条件</span>
                        <textarea
                          rows={3}
                          spellCheck={false}
                          autoCapitalize="off"
                          autoCorrect="off"
                          value={item.signal ?? ''}
                          placeholder="例如 C > A_CLOSE"
                          onChange={(event) => updateTrack(index, { signal: event.target.value })}
                        />
                      </label>
                      <label className="strategy-manage-field">
                        <span>确认得分</span>
                        <TrackingPointsInput
                          value={item.points ?? 0}
                          onChange={(points) => updateTrack(index, { points })}
                        />
                      </label>
                    </div>
                  </details>
                  <details className="strategy-rule-tracking-detail">
                    <summary>
                      <strong>计分保留到何时</strong>
                      <span>{item.score_until?.trim() ? '按表达式结束' : '默认保留至策略失效'}</span>
                    </summary>
                    <div className="strategy-rule-tracking-detail-body">
                      <label className="strategy-manage-field">
                        <span>计分结束条件</span>
                        <textarea
                          rows={3}
                          spellCheck={false}
                          autoCapitalize="off"
                          autoCorrect="off"
                          value={item.score_until ?? ''}
                          placeholder="例如 SCORE_AGE >= 5 或 C < MA(C, 10)"
                          onChange={(event) => updateTrack(index, { score_until: event.target.value })}
                        />
                      </label>
                      <p className="strategy-manage-note">
                        留空时，已得分超过观察期限仍保留，直到策略失效。填写后，条件为真当天停止并清零，优先于当天步骤和确认；例如 SCORE_AGE &gt;= 5 表示首次得分后第 5 日结束。
                      </p>
                    </div>
                  </details>
                  <details className="strategy-rule-editor-help">
                    <summary>跟踪说明与变量</summary>
                    <ul>
                      <li>起点当天为第 0 日，之后按市场交易日观察；原条件不再满足仍可跟踪。重复发现不换起点、不续期。</li>
                      <li>冻结值只在起点取一次，后续通过 A_变量引用。</li>
                      <li>永久失效会结束事件；当日资格不满足只跳过当天，不撤回已取得的分数。缺行情或必要指标也会结束事件并清零，原期限结束后才允许新建。</li>
                      <li>步骤按顺序完成，每天最多一步；步骤分当天取得并保留。确认条件最早在全部步骤完成后的下一市场交易日判断，留空表示符合资格即成立；确认分只增加一次，与步骤分相加。</li>
                      <li>观察期限只限制等待新步骤和确认；尚未得分的事件到期结束，已得分的事件保留原起点和分数，直到策略失效或计分结束条件成立。</li>
                      <li>AGE：距起点日数；SCORE_AGE：首次得分后日数；SIGNAL_AGE：首次确认后日数。尚未得分或确认时对应值为 -1。COMPLETED_STEPS：已完成步骤数；TRIGGER_SCORE：起点分值。</li>
                      <li>跟踪独立计分，在规则明细与回测中以「跟踪·名称」单列；与原策略分一起计入总分。保存后计算排名会更新结果，也可单独回放。分值可填负数，填 0 只观察。</li>
                    </ul>
                  </details>
                </>
              )}
            </div>
          ),
      )}
    </section>
  )
}
