import type { StrategyManageDistPoint } from '../apis/strategyManage'

export type DistPointRow = {
  min: string
  max: string
  points: string
}

export function validateDistPointRows(rows: DistPointRow[]) {
  const errors: Record<number, string> = {}
  const parsed: Array<StrategyManageDistPoint & { row: number }> = []
  if (rows.length === 0) {
    errors[-1] = '请至少添加一个区间'
  }
  rows.forEach((row, index) => {
    const min = Number(row.min)
    const max = Number(row.max)
    const points = Number(row.points)
    if (!row.min.trim() || !row.max.trim() || !row.points.trim()) {
      errors[index] = `第 ${index + 1} 行：请填完整起始日、结束日和分值`
    } else if (
      !Number.isSafeInteger(min) ||
      !Number.isSafeInteger(max) ||
      min < 0 ||
      max < 0
    ) {
      errors[index] = `第 ${index + 1} 行：报价日必须是大于等于 0 的整数`
    } else if (!Number.isFinite(points)) {
      errors[index] = `第 ${index + 1} 行：分值必须是合法数字`
    } else if (min > max) {
      errors[index] = `第 ${index + 1} 行：起始日不能大于结束日`
    } else {
      parsed.push({ min, max, points, row: index })
    }
  })
  parsed.sort((left, right) => left.min - right.min)
  // Compare against the furthest end, so nested intervals are also all marked.
  let previous = parsed[0]
  for (const current of parsed.slice(1)) {
    if (previous.max >= current.min) {
      errors[previous.row] =
        `第 ${previous.row + 1} 行与第 ${current.row + 1} 行区间重叠（含两端）`
      errors[current.row] =
        `第 ${current.row + 1} 行与第 ${previous.row + 1} 行区间重叠（含两端）`
    }
    if (current.max > previous.max) previous = current
  }
  return {
    points: parsed.map(({ min, max, points }) => ({ min, max, points })),
    errors,
  }
}
