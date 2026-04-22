import { useState } from 'react'
import {
  validateStrategyPaperValidationTemplateExpressions,
} from '../../../apis/strategyPaperValidation'
import '../css/IntradayTemplateManagerModal.css'

export type StrategyPaperValidationTemplate = {
  id: string
  name: string
  buyExpression: string
  sellExpression: string
}

type TemplateEditorMode = 'create' | 'edit'

type StrategyPaperValidationTemplateManagerModalProps = {
  open: boolean
  templates: StrategyPaperValidationTemplate[]
  onChangeTemplates: (nextTemplates: StrategyPaperValidationTemplate[]) => void
  onClose: () => void
  onTemplateRemoved?: (templateId: string) => void
  title?: string
}

function createId() {
  return typeof crypto !== 'undefined' &&
    typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
}

function createTemplate(
  name = '',
  buyExpression = '',
  sellExpression = '',
): StrategyPaperValidationTemplate {
  return {
    id: createId(),
    name,
    buyExpression,
    sellExpression,
  }
}

function summarizeExpression(expression: string, maxLength = 72) {
  const compact = expression.replace(/\s+/g, ' ').trim()
  if (compact.length <= maxLength) return compact || '--'
  return `${compact.slice(0, maxLength)}...`
}

export default function StrategyPaperValidationTemplateManagerModal({
  open,
  templates,
  onChangeTemplates,
  onClose,
  onTemplateRemoved,
  title = '表达式模板管理',
}: StrategyPaperValidationTemplateManagerModalProps) {
  const [templateEditorMode, setTemplateEditorMode] =
    useState<TemplateEditorMode>('create')
  const [templateEditorOriginalId, setTemplateEditorOriginalId] = useState('')
  const [templateEditorDraft, setTemplateEditorDraft] =
    useState<StrategyPaperValidationTemplate>(createTemplate())
  const [templateEditorNotice, setTemplateEditorNotice] = useState('')
  const [templateEditorError, setTemplateEditorError] = useState('')
  const [templateValidationMessage, setTemplateValidationMessage] = useState('')
  const [templateValidating, setTemplateValidating] = useState(false)

  function resetTemplateEditor() {
    setTemplateEditorMode('create')
    setTemplateEditorOriginalId('')
    setTemplateEditorDraft(createTemplate())
    setTemplateEditorNotice('')
    setTemplateEditorError('')
    setTemplateValidationMessage('')
  }

  function openTemplateEditorForEdit(template: StrategyPaperValidationTemplate) {
    setTemplateEditorMode('edit')
    setTemplateEditorOriginalId(template.id)
    setTemplateEditorDraft({ ...template })
    setTemplateEditorNotice('')
    setTemplateEditorError('')
    setTemplateValidationMessage('')
  }

  async function validateTemplateExpressionsCore(
    buyExpression: string,
    sellExpression: string,
  ) {
    const result = await validateStrategyPaperValidationTemplateExpressions(
      buyExpression,
      sellExpression,
    )
    return result.message
  }

  async function onValidateTemplateExpressions() {
    const buyExpression = templateEditorDraft.buyExpression.trim()
    const sellExpression = templateEditorDraft.sellExpression.trim()
    if (!buyExpression || !sellExpression) {
      setTemplateValidationMessage('')
      setTemplateEditorError('买点方程和卖点方程都不能为空')
      return
    }

    setTemplateValidating(true)
    setTemplateEditorError('')
    setTemplateEditorNotice('')
    try {
      const message = await validateTemplateExpressionsCore(
        buyExpression,
        sellExpression,
      )
      setTemplateValidationMessage(message)
      setTemplateEditorNotice('表达式校验通过')
    } catch (validationError) {
      setTemplateValidationMessage('')
      setTemplateEditorError(`表达式校验失败: ${String(validationError)}`)
    } finally {
      setTemplateValidating(false)
    }
  }

  async function onSaveTemplate() {
    const name = templateEditorDraft.name.trim()
    const buyExpression = templateEditorDraft.buyExpression.trim()
    const sellExpression = templateEditorDraft.sellExpression.trim()
    if (!name || !buyExpression || !sellExpression) {
      setTemplateEditorError('模板名称、买点方程和卖点方程都不能为空')
      return
    }

    setTemplateValidating(true)
    setTemplateEditorError('')
    setTemplateEditorNotice('')
    try {
      const message = await validateTemplateExpressionsCore(
        buyExpression,
        sellExpression,
      )
      setTemplateValidationMessage(message)
      setTemplateEditorNotice('表达式校验通过')
    } catch (validationError) {
      setTemplateValidationMessage('')
      setTemplateEditorError(`表达式校验失败: ${String(validationError)}`)
      return
    } finally {
      setTemplateValidating(false)
    }

    if (templateEditorMode === 'create') {
      onChangeTemplates([
        ...templates,
        {
          id: createId(),
          name,
          buyExpression,
          sellExpression,
        },
      ])
      setTemplateEditorNotice('模板已新增')
      setTemplateEditorError('')
      setTemplateValidationMessage('')
      setTemplateEditorDraft(createTemplate())
      return
    }

    onChangeTemplates(
      templates.map((item) =>
        item.id === templateEditorOriginalId
          ? { ...item, name, buyExpression, sellExpression }
          : item,
      ),
    )
    setTemplateEditorNotice('模板已更新')
    setTemplateEditorError('')
  }

  function removeTemplate(templateId: string) {
    onChangeTemplates(templates.filter((item) => item.id !== templateId))
    onTemplateRemoved?.(templateId)
    if (templateEditorOriginalId === templateId) {
      resetTemplateEditor()
    }
  }

  if (!open) {
    return null
  }

  return (
    <div className="intraday-template-modal-mask" onClick={onClose}>
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
                        ? 'intraday-template-modal-item intraday-template-modal-item-active'
                        : 'intraday-template-modal-item'
                    }
                  >
                    <div className="intraday-template-item-main">
                      <strong>{tpl.name}</strong>
                      <span>买点：{summarizeExpression(tpl.buyExpression)}</span>
                      <span>卖点：{summarizeExpression(tpl.sellExpression)}</span>
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
                {templateEditorMode === 'create' ? '新增模板' : '编辑模板'}
              </h5>
              <div className="intraday-template-editor-actions">
                <button
                  type="button"
                  onClick={() => void onValidateTemplateExpressions()}
                  disabled={templateValidating}
                >
                  {templateValidating ? '校验中...' : '表达式验证'}
                </button>
                <button type="button" onClick={() => void onSaveTemplate()}>
                  {templateEditorMode === 'create' ? '保存新增' : '保存更新'}
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
                placeholder="模板名，例如：TOP100 五日止盈"
              />
              <textarea
                value={templateEditorDraft.buyExpression}
                onChange={(event) =>
                  setTemplateEditorDraft((draft) => ({
                    ...draft,
                    buyExpression: event.target.value,
                  }))
                }
                placeholder="买点方程，例如：RANK <= 100"
              />
              <textarea
                value={templateEditorDraft.sellExpression}
                onChange={(event) =>
                  setTemplateEditorDraft((draft) => ({
                    ...draft,
                    sellExpression: event.target.value,
                  }))
                }
                placeholder="卖点方程，例如：TIME >= 5 OR RATEH >= 8"
              />
            </div>

            {templateValidationMessage ? (
              <div className="intraday-template-check intraday-template-check-ok">
                <div>{templateValidationMessage}</div>
                <div>校验通道：策略模拟盘买卖点表达式</div>
              </div>
            ) : null}

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
            买点字段：<code>C / O / H / L / V / PCT_CHG / TOTAL_MV_YI / RANK</code>
          </div>
          <div>
            卖点附加字段：<code>TIME / RATEO / RATEH</code>，分别表示持仓天数、开盘收益率和最高收益率。
          </div>
        </div>
      </div>
    </div>
  )
}
