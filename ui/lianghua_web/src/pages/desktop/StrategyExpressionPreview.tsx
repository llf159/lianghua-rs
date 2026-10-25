import { useLayoutEffect, useMemo, useRef, useState } from 'react'
import { formatStrategyExpression } from '../../shared/strategyExpression'

export default function StrategyExpressionPreview({ expression }: { expression: string }) {
  const ref = useRef<HTMLPreElement>(null)
  const [columns, setColumns] = useState(100)

  useLayoutEffect(() => {
    const element = ref.current
    if (!element) return
    const context = document.createElement('canvas').getContext('2d')
    if (!context) return
    const observer = new ResizeObserver(([entry]) => {
      const style = getComputedStyle(element)
      context.font = `${style.fontSize} ${style.fontFamily}`
      const characterWidth = context.measureText('0').width
      if (characterWidth > 0) {
        setColumns(Math.max(16, Math.min(100, Math.floor(entry.contentRect.width / characterWidth) - 1)))
      }
    })
    observer.observe(element)
    return () => observer.disconnect()
  }, [])

  const content = useMemo(() => {
    const formatted = formatStrategyExpression(expression, columns)
    const result = []
    let end = 0
    for (const match of formatted.matchAll(/#[^\r\n]*|\b(?:AND|OR|NOT|IN)\b|:=/gi)) {
      result.push(formatted.slice(end, match.index))
      result.push(
        <span key={match.index} className={match[0].startsWith('#') ? 'strategy-expression-comment' : 'strategy-expression-keyword'}>
          {match[0]}
        </span>,
      )
      end = match.index + match[0].length
    }
    result.push(formatted.slice(end))
    return result
  }, [expression, columns])

  return <pre ref={ref} className="strategy-manage-expression-preview"><code>{content}</code></pre>
}
