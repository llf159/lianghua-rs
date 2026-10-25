type ExpressionNode = string | { open: string; close: string; children: ExpressionNode[] }
type Layout = string | Layout[] | { group: Layout; width: number } | { indent: Layout } | { line: string; hard?: boolean }

function flatWidth(layout: Layout): number {
  if (typeof layout === 'string') return layout.length
  if (Array.isArray(layout)) return layout.reduce<number>((sum, child) => sum + flatWidth(child), 0)
  if ('group' in layout) return layout.width
  if ('indent' in layout) return flatWidth(layout.indent)
  return layout.hard ? Infinity : layout.line.length
}

function grouped(layout: Layout): Layout {
  return { group: layout, width: flatWidth(layout) }
}

function expressionLayout(nodes: ExpressionNode[], logicalContinuation = true): Layout {
  // Split only at this nesting level. OR binds less tightly than AND; function
  // arguments and arithmetic parentheses stay intact while the outer group breaks.
  for (const separator of [';', 'OR', 'AND', ',', ':=', 'IN', '=', '==', '!=', '<>', '>=', '<=', '>', '<', '+', '-', '*', '/']) {
    const parts: ExpressionNode[][] = [[]]
    const separators: string[] = []
    for (let index = 0; index < nodes.length; index += 1) {
      const node = nodes[index]
      const previous = nodes[index - 1]
      const unary = ['-', '+'].includes(separator)
        && (previous === undefined || (typeof previous === 'string' && /^(?:[-+*/=<>!]+|:=|NOT)$/i.test(previous)))
      if (typeof node === 'string' && node.toUpperCase() === separator && !unary) {
        separators.push(node)
        parts.push([])
      } else {
        parts[parts.length - 1].push(node)
      }
    }
    if (parts.length === 1) continue
    const first = expressionLayout(parts[0], logicalContinuation)
    const rest: Layout[] = []
    for (let index = 1; index < parts.length; index += 1) {
      const next = expressionLayout(parts[index], logicalContinuation)
      if (separator === ';') rest.push(';', { line: '', hard: true }, next)
      else if (separator === ',') rest.push(',', { line: ' ' }, next)
      else if (/^(?::=|IN|[=<>!]+)$/.test(separator)) rest.push(' ', separators[index - 1], { line: ' ' }, next)
      else rest.push({ line: ' ' }, separators[index - 1], ' ', next)
    }
    if (separator === ';') return [first, ...rest]
    const aligned = separator === ',' || (!logicalContinuation && (separator === 'AND' || separator === 'OR'))
    return grouped([first, aligned ? rest : { indent: rest }])
  }

  const layout: Layout[] = []
  let previous: ExpressionNode | undefined
  let unary = false
  for (const node of nodes) {
    if (typeof node === 'string' && node.startsWith('#')) {
      if (layout.length > 0) layout.push(' ')
      layout.push(node, { line: '', hard: true })
      previous = undefined
      unary = false
      continue
    }
    const isUnary = typeof node === 'string' && ['-', '+'].includes(node)
      && (previous === undefined || (typeof previous === 'string' && /^(?:[-+*/=<>!]+|:=|NOT)$/i.test(previous)))
    const functionCall = typeof node !== 'string' && node.open === '('
      && typeof previous === 'string' && /^[A-Za-z_][A-Za-z_0-9]*$/.test(previous)
      && !/^(NOT|IN)$/i.test(previous)
    if (previous !== undefined && !unary && !functionCall) layout.push(' ')
    if (typeof node === 'string') {
      layout.push(node)
    } else {
      layout.push(grouped([
        node.open,
        { indent: [{ line: '' }, expressionLayout(node.children, false)] },
        { line: '' },
        node.close,
      ]))
    }
    previous = node
    unary = isUnary
  }
  return grouped(layout)
}

// Presentation only: strategy files are also used as calculation fingerprints.
export function formatStrategyExpression(expression: string, maxWidth = 100): string {
  const tokens = expression.match(/#[^\r\n]*|[A-Za-z_][A-Za-z_0-9]*|(?:\d+(?:\.\d*)?|\.\d+)|:=|>=|<=|!=|<>|==|[^\s]/g) ?? []
  const nodes: ExpressionNode[] = []
  const stack = [nodes]
  const closes: string[] = []
  for (const token of tokens) {
    if (token === '(' || token === '[') {
      const children: ExpressionNode[] = []
      const close = token === '(' ? ')' : ']'
      stack[stack.length - 1].push({ open: token, close, children })
      stack.push(children)
      closes.push(close)
    } else if (token === ')' || token === ']') {
      if (closes.pop() !== token) return expression
      stack.pop()
    } else {
      stack[stack.length - 1].push(token)
    }
  }
  if (closes.length > 0) return expression

  const lines: string[] = []
  let line = ''
  // Explicit work stack avoids recursive rendering and lets each nested group
  // independently stay flat when it fits the remaining measured line width.
  const work = [{ layout: expressionLayout(nodes), indent: 0, flat: false }]
  while (work.length > 0) {
    const { layout, indent, flat } = work.pop()!
    if (typeof layout === 'string') {
      line += layout
    } else if (Array.isArray(layout)) {
      for (let index = layout.length - 1; index >= 0; index -= 1) {
        work.push({ layout: layout[index], indent, flat })
      }
    } else if ('group' in layout) {
      let fits = flat
      if (!flat && line.length + layout.width <= maxWidth) {
        // Include the pending suffix (such as ");" or " >=") in the fit
        // decision, rather than letting it overflow an otherwise fitting group.
        let remaining = maxWidth - line.length
        const probe = [...work, { layout: layout.group, indent, flat: true }]
        while (remaining >= 0 && probe.length > 0) {
          const candidate = probe.pop()!
          const next = candidate.layout
          if (typeof next === 'string') remaining -= next.length
          else if (Array.isArray(next)) {
            for (let index = next.length - 1; index >= 0; index -= 1) {
              probe.push({ ...candidate, layout: next[index] })
            }
          } else if ('group' in next) probe.push({ ...candidate, layout: next.group, flat: true })
          else if ('indent' in next) probe.push({ ...candidate, layout: next.indent })
          else if (next.hard || !candidate.flat) break
          else remaining -= next.line.length
        }
        fits = remaining >= 0
      }
      work.push({ layout: layout.group, indent, flat: fits })
    } else if ('indent' in layout) {
      work.push({ layout: layout.indent, indent: indent + 2, flat })
    } else if (flat && !layout.hard) {
      line += layout.line
    } else {
      if (line.trim()) lines.push(line.trimEnd())
      line = ' '.repeat(indent)
    }
  }
  if (line.trim()) lines.push(line.trimEnd())
  return lines.join('\n')
}
