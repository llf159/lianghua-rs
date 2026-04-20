import { useEffect, type ReactNode } from 'react'
import './confirmDialog.css'

type ConfirmDialogProps = {
  open: boolean
  title: string
  message?: ReactNode
  confirmText?: string
  cancelText?: string
  danger?: boolean
  busy?: boolean
  onConfirm: () => void
  onCancel: () => void
}

export default function ConfirmDialog({
  open,
  title,
  message,
  confirmText = '确认',
  cancelText = '取消',
  danger = false,
  busy = false,
  onConfirm,
  onCancel,
}: ConfirmDialogProps) {
  useEffect(() => {
    if (!open) {
      return
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape' && !busy) {
        onCancel()
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [busy, onCancel, open])

  if (!open) {
    return null
  }

  return (
    <div
      className="confirm-dialog-backdrop"
      role="presentation"
      onClick={(event) => {
        if (event.target === event.currentTarget && !busy) {
          onCancel()
        }
      }}
    >
      <section className="confirm-dialog" role="dialog" aria-modal="true" aria-label={title}>
        <h3>{title}</h3>
        {message ? <p className="confirm-dialog-message">{message}</p> : null}

        <div className="confirm-dialog-actions">
          <button className="confirm-dialog-btn" type="button" onClick={onCancel} disabled={busy}>
            {cancelText}
          </button>
          <button
            className={danger ? 'confirm-dialog-btn confirm-dialog-btn-danger' : 'confirm-dialog-btn confirm-dialog-btn-primary'}
            type="button"
            onClick={onConfirm}
            disabled={busy}
          >
            {confirmText}
          </button>
        </div>
      </section>
    </div>
  )
}
