import { useEffect, useState } from "react";
import {
  type ExpressionFieldData,
  type IntradayMonitorTemplate,
  getExpressionCapabilities,
  validateIntradayMonitorTemplateExpression,
} from "../../../apis/reader";
import "../css/IntradayTemplateManagerModal.css";

type TemplateEditorMode = "create" | "edit";

type IntradayTemplateManagerModalProps = {
  open: boolean;
  sourcePath: string;
  templates: IntradayMonitorTemplate[];
  onChangeTemplates: (nextTemplates: IntradayMonitorTemplate[]) => void;
  onClose: () => void;
  onTemplateRemoved?: (templateId: string) => void;
  title?: string;
};

function createId() {
  return typeof crypto !== "undefined" &&
    typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function createTemplate(name = "", expression = ""): IntradayMonitorTemplate {
  return {
    id: createId(),
    name,
    expression,
    enabled: true,
  };
}

function summarizeExpression(expression: string, maxLength = 96) {
  const compact = expression.replace(/\s+/g, " ").trim();
  if (compact.length <= maxLength) return compact || "--";
  return `${compact.slice(0, maxLength)}...`;
}

export default function IntradayTemplateManagerModal({
  open,
  sourcePath,
  templates,
  onChangeTemplates,
  onClose,
  onTemplateRemoved,
  title = "模板管理",
}: IntradayTemplateManagerModalProps) {
  const [templateEditorMode, setTemplateEditorMode] =
    useState<TemplateEditorMode>("create");
  const [templateEditorOriginalId, setTemplateEditorOriginalId] = useState("");
  const [templateEditorDraft, setTemplateEditorDraft] =
    useState<IntradayMonitorTemplate>(createTemplate(""));
  const [templateEditorNotice, setTemplateEditorNotice] = useState("");
  const [templateEditorError, setTemplateEditorError] = useState("");
  const [templateValidating, setTemplateValidating] = useState(false);
  const [realtimeFields, setRealtimeFields] = useState<ExpressionFieldData[]>(
    [],
  );

  useEffect(() => {
    if (!open) return;
    let active = true;
    void getExpressionCapabilities()
      .then((capabilities) => {
        if (active) setRealtimeFields(capabilities.intradayRealtimeFields);
      })
      .catch(() => {
        if (active) setRealtimeFields([]);
      });
    return () => {
      active = false;
    };
  }, [open]);

  const sourcePathTrimmed = sourcePath.trim();
  const enabledTemplateCount = templates.filter(
    (template) => template.enabled,
  ).length;

  function resetTemplateEditor() {
    setTemplateEditorMode("create");
    setTemplateEditorOriginalId("");
    setTemplateEditorDraft(createTemplate(""));
    setTemplateEditorNotice("");
    setTemplateEditorError("");
  }

  function openTemplateEditorForEdit(template: IntradayMonitorTemplate) {
    setTemplateEditorMode("edit");
    setTemplateEditorOriginalId(template.id);
    setTemplateEditorDraft({ ...template });
    setTemplateEditorNotice("");
    setTemplateEditorError("");
  }

  async function validateTemplateExpressionCore(expression: string) {
    if (!sourcePathTrimmed) {
      throw new Error("请先完成数据目录加载");
    }
    const result = await validateIntradayMonitorTemplateExpression(
      sourcePathTrimmed,
      expression,
    );
    return result.message;
  }

  async function onValidateTemplateExpression() {
    const expression = templateEditorDraft.expression.trim();
    if (!expression) {
      setTemplateEditorError("请先填写模板表达式");
      return;
    }

    setTemplateValidating(true);
    setTemplateEditorError("");
    setTemplateEditorNotice("");
    try {
      await validateTemplateExpressionCore(expression);
      setTemplateEditorNotice("表达式校验通过");
    } catch (validationError) {
      setTemplateEditorError(`表达式校验失败: ${String(validationError)}`);
    } finally {
      setTemplateValidating(false);
    }
  }

  async function onSaveTemplate() {
    const name = templateEditorDraft.name.trim();
    const expression = templateEditorDraft.expression.trim();
    if (!name || !expression) {
      setTemplateEditorError("模板名称和表达式都不能为空");
      return;
    }

    setTemplateValidating(true);
    setTemplateEditorError("");
    setTemplateEditorNotice("");
    try {
      await validateTemplateExpressionCore(expression);
      setTemplateEditorNotice("");
    } catch (validationError) {
      setTemplateEditorError(`表达式校验失败: ${String(validationError)}`);
      return;
    } finally {
      setTemplateValidating(false);
    }

    if (templateEditorMode === "create") {
      onChangeTemplates([
        ...templates,
        {
          id: createId(),
          name,
          expression,
          enabled: true,
        },
      ]);
      setTemplateEditorNotice("模板已新增");
      setTemplateEditorError("");
      setTemplateEditorDraft(createTemplate(""));
      return;
    }

    onChangeTemplates(
      templates.map((item) =>
        item.id === templateEditorOriginalId
          ? { ...item, name, expression }
          : item,
      ),
    );
    setTemplateEditorNotice("模板已更新");
    setTemplateEditorError("");
  }

  function removeTemplate(templateId: string) {
    onChangeTemplates(templates.filter((item) => item.id !== templateId));
    onTemplateRemoved?.(templateId);
    if (templateEditorOriginalId === templateId) {
      resetTemplateEditor();
    }
  }

  function toggleTemplate(templateId: string) {
    onChangeTemplates(
      templates.map((template) =>
        template.id === templateId
          ? { ...template, enabled: !template.enabled }
          : template,
      ),
    );
  }

  if (!open) {
    return null;
  }

  return (
    <div className="intraday-template-modal-mask" onClick={onClose}>
      <div
        className="intraday-template-modal"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="intraday-template-modal-head">
          <div className="intraday-template-modal-title">
            <div
              className="intraday-template-modal-title-icon"
              aria-hidden="true"
            >
              T
            </div>
            <div>
              <h4>{title}</h4>
              <p>只有已开启的策略会参与实时判断</p>
            </div>
          </div>
          <button
            type="button"
            className="intraday-template-modal-close"
            onClick={onClose}
          >
            关闭
          </button>
        </div>

        <div className="intraday-template-workspace">
          <section className="intraday-template-list-panel">
            <div className="intraday-template-panel-head">
              <div>
                <h5>模板策略</h5>
                <span className="intraday-template-count">
                  {enabledTemplateCount} 个启用 · 共 {templates.length} 个
                </span>
              </div>
              <button
                type="button"
                className="intraday-template-create-btn"
                onClick={resetTemplateEditor}
              >
                新建模板
              </button>
            </div>
            <div className="intraday-template-modal-list">
              {templates.length === 0 ? (
                <div className="intraday-template-empty">暂无模板</div>
              ) : (
                templates.map((tpl) => (
                  <div
                    key={tpl.id}
                    className={[
                      "intraday-template-modal-item",
                      "is-card-editable",
                      "has-switch",
                      templateEditorOriginalId === tpl.id
                        ? "intraday-template-modal-item-active"
                        : "",
                      tpl.enabled ? "" : "is-disabled",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                  >
                    <button
                      type="button"
                      className="intraday-template-item-edit-target"
                      aria-label={`编辑策略 ${tpl.name}`}
                      onClick={() => openTemplateEditorForEdit(tpl)}
                    />
                    <button
                      type="button"
                      className={
                        tpl.enabled
                          ? "intraday-template-switch is-active"
                          : "intraday-template-switch"
                      }
                      role="switch"
                      aria-checked={tpl.enabled}
                      aria-label={`${tpl.name}${tpl.enabled ? "关闭" : "开启"}实时判断`}
                      title={tpl.enabled ? "关闭实时判断" : "开启实时判断"}
                      onClick={(event) => {
                        event.stopPropagation();
                        toggleTemplate(tpl.id);
                      }}
                    >
                      <span />
                    </button>
                    <div className="intraday-template-item-main">
                      <div className="intraday-template-item-title">
                        <strong>{tpl.name}</strong>
                        <span
                          className={
                            tpl.enabled
                              ? "intraday-template-status is-enabled"
                              : "intraday-template-status"
                          }
                        >
                          {tpl.enabled ? "判断中" : "已停用"}
                        </span>
                      </div>
                      <span>{summarizeExpression(tpl.expression, 96)}</span>
                    </div>
                    <div className="intraday-template-item-actions">
                      <button
                        type="button"
                        className="intraday-template-action-btn is-danger"
                        onClick={(event) => {
                          event.stopPropagation();
                          removeTemplate(tpl.id);
                        }}
                      >
                        删除
                      </button>
                    </div>
                  </div>
                ))
              )}
            </div>
          </section>

          <section className="intraday-template-editor-panel">
            <div className="intraday-template-panel-head">
              <h5>
                {templateEditorMode === "create" ? "新增模板" : "编辑模板"}
              </h5>
              <div className="intraday-template-editor-actions">
                <button
                  type="button"
                  className="intraday-template-validate-btn"
                  onClick={() => void onValidateTemplateExpression()}
                  disabled={templateValidating}
                >
                  {templateValidating ? "校验中..." : "表达式验证"}
                </button>
                <button
                  type="button"
                  className="intraday-template-save-btn"
                  onClick={() => void onSaveTemplate()}
                  disabled={templateValidating}
                >
                  {templateEditorMode === "create" ? "保存新增" : "保存更新"}
                </button>
              </div>
            </div>

            <div className="intraday-template-modal-form">
              <label>
                <span>模板名称</span>
                <input
                  value={templateEditorDraft.name}
                  onChange={(event) =>
                    setTemplateEditorDraft((draft) => ({
                      ...draft,
                      name: event.target.value,
                    }))
                  }
                  placeholder="例如：放量突破"
                />
              </label>
              <label>
                <span>判断表达式</span>
                <textarea
                  value={templateEditorDraft.expression}
                  onChange={(event) =>
                    setTemplateEditorDraft((draft) => ({
                      ...draft,
                      expression: event.target.value,
                    }))
                  }
                  placeholder="示例：C > RT_AVG AND RT_VR >= 2"
                />
              </label>
            </div>

            {templateEditorNotice ? (
              <div className="intraday-template-check intraday-template-check-ok">
                {templateEditorNotice}
              </div>
            ) : null}

            {templateEditorError ? (
              <div className="intraday-template-check intraday-template-check-error">
                {templateEditorError}
              </div>
            ) : null}
          </section>
        </div>

        <div className="intraday-template-tip-block">
          <div>
            常用字段：
            <code>C / O / H / L / V / PCT_CHG / TOR / TOTAL_MV_YI / ZHANG</code>
          </div>
          <div>
            指标字段：可直接引用 <code>stock_data</code> 已落库指标列，或{" "}
            <code>ind.toml</code> 中定义的指标名。
          </div>
          {realtimeFields.length > 0 ? (
            realtimeFields.map((field) => (
              <div key={field.name}>
                <code>{field.name}</code>：{field.description} 示例：
                <code>{field.example}</code>
              </div>
            ))
          ) : (
            <div>实时字段说明暂不可用，请以表达式验证结果为准。</div>
          )}
          <div>
            切换模板后可点“仅刷新标记”，基于已有实时行情快照重算标签，无需重新拉取行情。
          </div>
        </div>
      </div>
    </div>
  );
}
