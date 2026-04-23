import { useState } from 'react'
import '../css/IntradayTemplateManagerModal.css'

export type ExpressionStockPickTemplate = {
  id: string
  name: string
  expression: string
}

type TemplateEditorMode = 'create' | 'edit'

type ExpressionStockPickTemplateManagerModalProps = {
  open: boolean
  templates: ExpressionStockPickTemplate[]
  initialExpression: string
  onChangeTemplates: (nextTemplates: ExpressionStockPickTemplate[]) => void
  onClose: () => void
  onTemplateRemoved?: (templateId: string) => void
}

function createId() {
  return typeof crypto !== 'undefined' &&
    typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
}

function createTemplate(name = '', expression = ''): ExpressionStockPickTemplate {
  return {
    id: createId(),
    name,
    expression,
  }
}

function summarizeExpression(expression: string, maxLength = 96) {
  const compact = expression.replace(/\s+/g, ' ').trim()
  if (compact.length <= maxLength) return compact || '--'
  return `${compact.slice(0, maxLength)}...`
}

export default function ExpressionStockPickTemplateManagerModal({
  open,
  templates,
  initialExpression,
  onChangeTemplates,
  onClose,
  onTemplateRemoved,
}: ExpressionStockPickTemplateManagerModalProps) {
  const [templateEditorMode, setTemplateEditorMode] =
    useState<TemplateEditorMode>('create')
  const [templateEditorOriginalId, setTemplateEditorOriginalId] = useState('')
  const [templateEditorDraft, setTemplateEditorDraft] =
    useState<ExpressionStockPickTemplate>(createTemplate('', initialExpression))
  const [templateEditorNotice, setTemplateEditorNotice] = useState('')
  const [templateEditorError, setTemplateEditorError] = useState('')

  function resetTemplateEditor() {
    setTemplateEditorMode('create')
    setTemplateEditorOriginalId('')
    setTemplateEditorDraft(createTemplate('', initialExpression))
    setTemplateEditorNotice('')
    setTemplateEditorError('')
  }

  function openTemplateEditorForEdit(template: ExpressionStockPickTemplate) {
    setTemplateEditorMode('edit')
    setTemplateEditorOriginalId(template.id)
    setTemplateEditorDraft({ ...template })
    setTemplateEditorNotice('')
    setTemplateEditorError('')
  }

  function onSaveTemplate() {
    const name = templateEditorDraft.name.trim()
    const expression = templateEditorDraft.expression.trim()
    if (!name || !expression) {
      setTemplateEditorError('模板名称和表达式都不能为空')
      setTemplateEditorNotice('')
      return
    }

    if (templateEditorMode === 'create') {
      onChangeTemplates([
        ...templates,
        {
          id: createId(),
          name,
          expression,
        },
      ])
      setTemplateEditorNotice('模板已新增')
      setTemplateEditorError('')
      setTemplateEditorDraft(createTemplate('', initialExpression))
      return
    }

    onChangeTemplates(
      templates.map((item) =>
        item.id === templateEditorOriginalId
          ? { ...item, name, expression }
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
          <h4>表达式模板管理</h4>
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
                      <span>{summarizeExpression(tpl.expression)}</span>
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
                <button type="button" onClick={onSaveTemplate}>
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
                placeholder="模板名，例如：RANK 前 100"
              />
              <textarea
                value={templateEditorDraft.expression}
                onChange={(event) =>
                  setTemplateEditorDraft((draft) => ({
                    ...draft,
                    expression: event.target.value,
                  }))
                }
                placeholder="表达式，例如：RANK <= 100 AND PCT_CHG > 0"
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
            模板只保存表达式内容；选股范围、参考日和窗口参数仍使用页面当前配置。
          </div>
        </div>
      </div>
    </div>
  )
}
