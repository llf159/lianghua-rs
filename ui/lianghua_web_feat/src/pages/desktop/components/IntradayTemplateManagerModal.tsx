import { useState } from "react";
import {
  type IntradayMonitorTemplate,
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

  const sourcePathTrimmed = sourcePath.trim();

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

  if (!open) {
    return null;
  }

  return (
    <div
      className="intraday-template-modal-mask"
      onClick={onClose}
    >
      <div
        className="intraday-template-modal"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="intraday-template-modal-head">
          <h4>{title}</h4>
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
              <h5>模板列表</h5>
              <button type="button" onClick={resetTemplateEditor}>
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
                    className={
                      templateEditorOriginalId === tpl.id
                        ? "intraday-template-modal-item intraday-template-modal-item-active"
                        : "intraday-template-modal-item"
                    }
                  >
                    <div className="intraday-template-item-main">
                      <strong>{tpl.name}</strong>
                      <span>{summarizeExpression(tpl.expression, 96)}</span>
                    </div>
                    <div className="intraday-template-item-actions">
                      <button
                        type="button"
                        onClick={() => openTemplateEditorForEdit(tpl)}
                      >
                        编辑
                      </button>
                      <button
                        type="button"
                        onClick={() => removeTemplate(tpl.id)}
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
                  onClick={() => void onValidateTemplateExpression()}
                  disabled={templateValidating}
                >
                  {templateValidating ? "校验中..." : "表达式验证"}
                </button>
                <button type="button" onClick={() => void onSaveTemplate()}>
                  {templateEditorMode === "create" ? "保存新增" : "保存更新"}
                </button>
              </div>
            </div>

            <div className="intraday-template-modal-form">
              <input
                value={templateEditorDraft.name}
                onChange={(event) =>
                  setTemplateEditorDraft((draft) => ({
                    ...draft,
                    name: event.target.value,
                  }))
                }
                placeholder="模板名，例如：放量突破"
              />
              <textarea
                value={templateEditorDraft.expression}
                onChange={(event) =>
                  setTemplateEditorDraft((draft) => ({
                    ...draft,
                    expression: event.target.value,
                  }))
                }
                placeholder="示例：C > MA(C, 5) AND REALTIME_VOL_RATIO >= 2"
              />
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
            常用字段：<code>C / O / H / L / V / PCT_CHG / TOR / TOTAL_MV_YI / ZHANG</code>
          </div>
          <div>
            指标字段：可直接引用 <code>stock_data</code> 已落库指标列，或 <code>ind.toml</code> 中定义的指标名。
          </div>
          <div>
            实时字段：<code>REALTIME_CHANGE_OPEN_PCT / REALTIME_FALL_FROM_HIGH_PCT / REALTIME_VOL_RATIO / VOL_RATIO</code>
          </div>
          <div>
            高点回落：<code>REALTIME_FALL_FROM_HIGH_PCT</code> 为非负百分比值，<code>0</code> 表示当前价等于今日高点，不会返回负数；计算口径 = max((今日高点 - 当前价) / 今日高点, 0) × 100%
          </div>
          <div>
            量比基准：<code>REALTIME_VOL_RATIO</code> = 当前实时累计成交量 ÷ 最新历史日 <code>vol</code>
          </div>
          <div>
            切换模板后可点“仅刷新标记”，基于已有实时行情快照重算标签，无需重新拉取行情。
          </div>
        </div>
      </div>
    </div>
  );
}
