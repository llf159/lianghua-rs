import { useMemo } from 'react'
import { filterConceptItems } from '../shared/conceptExclusions'

type ConceptSelectionTone = 'primary' | 'warn' | 'neutral'

function toneClassName(tone: ConceptSelectionTone) {
  if (tone === 'warn') {
    return 'stock-pick-chip-btn is-warn'
  }
  if (tone === 'neutral') {
    return 'stock-pick-chip-btn is-neutral'
  }
  return 'stock-pick-chip-btn is-active'
}

export function normalizeStringArray(values: readonly string[]) {
  return filterConceptItems(values, [])
}

export function buildAvailableConceptOptions(
  conceptOptions: readonly string[],
  excludedConcepts: readonly string[],
) {
  return filterConceptItems(conceptOptions, excludedConcepts)
}

export function toggleStringSelection(values: readonly string[], value: string) {
  return values.includes(value)
    ? values.filter((item) => item !== value)
    : [...values, value]
}

function ConceptFilterPanel({
  title,
  selectedItems,
  availableItems,
  onToggle,
  onClear,
  keyword,
  onKeywordChange,
  activeTone = 'primary',
  clearLabel = '清空',
  emptyText = '没有匹配的概念。',
  panelClassName,
}: {
  title: string
  selectedItems: string[]
  availableItems: string[]
  onToggle: (value: string) => void
  onClear: () => void
  keyword: string
  onKeywordChange: (value: string) => void
  activeTone?: ConceptSelectionTone
  clearLabel?: string
  emptyText?: string
  panelClassName?: string
}) {
  const filteredItems = useMemo(() => {
    const needle = keyword.trim().toLowerCase()
    if (!needle) {
      return availableItems
    }
    return availableItems.filter((item) => item.toLowerCase().includes(needle))
  }, [availableItems, keyword])

  return (
    <div className={panelClassName ? `stock-pick-concept-panel ${panelClassName}` : 'stock-pick-concept-panel'}>
      <div className="stock-pick-concept-head">
        <strong>{title}</strong>
        <span>已选 {selectedItems.length} 项</span>
      </div>
      <div className="stock-pick-concept-toolbar">
        <input
          type="text"
          value={keyword}
          onChange={(event) => onKeywordChange(event.target.value)}
          placeholder="搜索概念"
          className="stock-pick-concept-search"
        />
        <button
          type="button"
          className="stock-pick-chip-btn"
          onClick={() => onKeywordChange('')}
          disabled={!keyword.trim()}
        >
          清空搜索
        </button>
        <button
          type="button"
          className="stock-pick-chip-btn"
          onClick={onClear}
          disabled={selectedItems.length === 0}
        >
          {clearLabel}
        </button>
      </div>
      <div className="stock-pick-concept-list">
        {filteredItems.length > 0 ? (
          filteredItems.map((item) => {
            const active = selectedItems.includes(item)
            return (
              <button
                key={item}
                type="button"
                className={active ? toneClassName(activeTone) : 'stock-pick-chip-btn'}
                onClick={() => onToggle(item)}
              >
                {item}
              </button>
            )
          })
        ) : (
          <span className="stock-pick-note">{emptyText}</span>
        )}
      </div>
    </div>
  )
}

export function ConceptIncludeExcludePanels({
  includeConcepts,
  excludeConcepts,
  availableConceptOptions,
  keyword,
  onKeywordChange,
  onToggleInclude,
  onToggleExclude,
  onClearInclude,
  onClearExclude,
  panelClassName,
}: {
  includeConcepts: string[]
  excludeConcepts: string[]
  availableConceptOptions: string[]
  keyword: string
  onKeywordChange: (value: string) => void
  onToggleInclude: (value: string) => void
  onToggleExclude: (value: string) => void
  onClearInclude: () => void
  onClearExclude: () => void
  panelClassName?: string
}) {
  return (
    <div className="stock-pick-concept-grid">
      <ConceptFilterPanel
        title="包含概念"
        selectedItems={includeConcepts}
        availableItems={availableConceptOptions}
        onToggle={onToggleInclude}
        onClear={onClearInclude}
        keyword={keyword}
        onKeywordChange={onKeywordChange}
        clearLabel="清空包含"
        panelClassName={panelClassName}
      />
      <ConceptFilterPanel
        title="排除概念"
        selectedItems={excludeConcepts}
        availableItems={availableConceptOptions}
        onToggle={onToggleExclude}
        onClear={onClearExclude}
        keyword={keyword}
        onKeywordChange={onKeywordChange}
        activeTone="warn"
        clearLabel="清空排除"
        panelClassName={panelClassName}
      />
    </div>
  )
}
