import { invoke } from '@tauri-apps/api/core'
import { listen } from '@tauri-apps/api/event'
import { useEffect, useState } from 'react'

export function beginTaskGroup(taskId: string, sourcePath: string) {
  return invoke<void>('begin_task_group', { taskId, sourcePath })
}

export function finishTaskGroup(taskId: string) {
  return invoke<void>('finish_task_group', { taskId })
}

export function useTaskControl(taskId: string | null) {
  const [state, setState] = useState({ taskId: '', requested: false, message: '' })
  useEffect(() => {
    if (!taskId) return
    let disposed = false
    let unlisten: (() => void) | undefined
    void listen<{ taskId: string; message: string }>('task-control-status', ({ payload }) => {
      if (payload.taskId === taskId && payload.message !== '正在执行任务') {
        setState((current) => ({ taskId, requested: current.taskId === taskId && current.requested, message: payload.message }))
      }
    }).then((stop) => { if (disposed) stop(); else unlisten = stop })
    return () => { disposed = true; unlisten?.() }
  }, [taskId])
  return {
    disabled: !taskId || (state.taskId === taskId && state.requested),
    message: state.taskId === taskId ? state.message : '',
    stop: async () => {
      if (!taskId) return
      setState({ taskId, requested: true, message: '正在请求停止，已提交结果会保留' })
      try {
        await invoke<void>('control_task', { taskId, rollback: false })
      } catch (error) {
        setState({ taskId, requested: false, message: `停止请求失败：${String(error)}` })
      }
    },
  }
}
