import { useEffect, useState } from 'react'
import { listen } from '@tauri-apps/api/event'
import { cloudPathLabel, deleteCloudVersion, estimateCloudUpload, listCloudData, syncCloud, type CloudItem, type UploadEstimate } from '../../../apis/cloudSync'
import CloudSyncSettingsModal from './CloudSyncSettingsModal'
import '../css/CloudSync.css'

function bytes(value: number) {
  if (value < 1024) return `${value} B`
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`
  return `${(value / 1024 / 1024).toFixed(2)} MB`
}

export default function CloudSyncPanel({ disabled, onSynced, onBusyChange }: { disabled: boolean; onSynced: () => void; onBusyChange: (busy: boolean) => void }) {
  const [busy, setBusy] = useState(false)
  const [estimate, setEstimate] = useState<UploadEstimate | null>(null)
  const [progress, setProgress] = useState('')
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [conflicts, setConflicts] = useState<string[]>([])
  const [manager, setManager] = useState(false)
  const [items, setItems] = useState<CloudItem[]>([])
  const [filter, setFilter] = useState('')
  const [showHistory, setShowHistory] = useState(false)
  const [selected, setSelected] = useState<CloudItem | null>(null)
  const [action, setAction] = useState<'restore' | 'delete' | null>(null)
  const [settings, setSettings] = useState(false)
  useEffect(() => {
    let cancelled = false
    const subscription = listen<string>('cloud-sync-progress', event => { if (!cancelled) setProgress(event.payload) })
    return () => { cancelled = true; void subscription.then(unlisten => unlisten()) }
  }, [])
  async function operate(kind: 'sync' | 'list' | 'restore' | 'delete' | 'estimate', item?: CloudItem) {
    if (busy || disabled) return
    setBusy(true); onBusyChange(true); setError(''); setNotice(''); setProgress(kind === 'list' ? '正在读取云端目录…' : '正在准备…')
    try {
      if (kind === 'estimate') {
        setEstimate(await estimateCloudUpload())
      } else if (kind === 'list') {
        setItems(await listCloudData()); setManager(true)
      } else if (kind === 'delete' && item) {
        await deleteCloudVersion(item)
        setNotice(`已删除云端版本：${item.path}；本地副本保留。`)
        setItems(await listCloudData())
      } else {
        const result = await syncCloud(kind === 'restore' ? item : undefined)
        setConflicts(result.conflicts)
        setNotice(`完成：上传 ${result.uploaded} 项，接收 ${result.downloaded} 项，跳过 ${result.skipped} 项；上传流量 ${bytes(result.uploadedBytes)}，下载 ${bytes(result.downloadedBytes)}。${result.warnings.length ? ` ${result.warnings.join('；')}` : ''}`)
        onSynced()
        if (manager) setItems(await listCloudData())
      }
      setSelected(null); setAction(null)
    } catch (e) { setError(String(e)) } finally { setBusy(false); onBusyChange(false); setProgress('') }
  }
  const visible = items.filter(item => (showHistory || item.isHead) && `${item.path} ${cloudPathLabel(item.path)}`.toLowerCase().includes(filter.toLowerCase()))
  return (
    <section className="settings-card cloud-panel" aria-label="云端同步">
      <div className="settings-head">
        <div><h3>云端同步</h3><p className="settings-section-note">策略、自选、指标图表配置、浏览习惯分四个包；总榜一个近五日快照，相似榜一个最新一天快照。</p></div>
        <div className="settings-actions">
          <button className="settings-primary-btn" type="button" disabled={busy || disabled} onClick={() => void operate('sync')}>{busy ? '处理中…' : '同步云端'}</button>
          <button className="settings-secondary-btn" type="button" disabled={busy || disabled} onClick={() => void operate('list')}>云端数据管理</button>
          <button className="settings-secondary-btn" type="button" disabled={busy || disabled} onClick={() => void operate('estimate')}>测算上传量</button>
          <button className="settings-secondary-btn" type="button" disabled={busy || disabled} onClick={() => setSettings(true)}>连接配置</button>
        </div>
      </div>
      {progress && <p className="cloud-notice" role="status">{progress}</p>}
      {notice && <p className="cloud-notice" role="status">{notice}</p>}
      {estimate && <div className="cloud-notice" role="status">
        <p>首次压缩上传约 {bytes(estimate.initialUploadBytes)}，共 {estimate.uploadObjects} 个有数据的分组/数据库快照。</p>
        <p>每次总榜五日快照约 {bytes(estimate.rankingDailyBytes)}，相似榜单日快照约 {bytes(estimate.similarityDailyBytes)}；按每天上传一份完整的新数据库快照估算，22 天约 {bytes(estimate.monthly22DaysBytes)}，31 天按当前快照大小估算约 {bytes(estimate.monthly31DaysBytes)}。</p>
        {estimate.missingSimilarity && <p>本机暂无相似榜结果，以上未包含相似榜流量。生成后请重新测算。</p>}
        <p>按每月 1 GB 上传额度，已测算部分占约 {(estimate.monthly31DaysBytes / 1_000_000_000 * 100).toFixed(1)}%。{estimate.monthly31DaysBytes > 1_000_000_000 ? '当前估算已超过月额度。' : estimate.missingSimilarity ? `每天上传的相似榜快照需低于约 ${bytes(Math.max(0, (1_000_000_000 - estimate.monthly31DaysBytes - estimate.initialUploadBytes) / 31))}，并给重算、配置修改和索引留出余量。` : '仍需给额外重算和索引请求留出余量。'}</p>
        <p>测算不连接网盘。月估算包含当前配置，首月首次快照上传另计；反复重算产生新内容、修改策略及请求索引会增加流量。删除旧日期不会返还已用上传额度。</p>
      </div>}
      {error && <p className="cloud-error" role="alert">{error}</p>}
      {conflicts.length > 0 && <div className="cloud-conflicts"><strong>以下内容存在分支或同步期间被修改，已保留各份数据：</strong><ul>{conflicts.map(path => <li key={path}>{cloudPathLabel(path)}</li>)}</ul><p>进入云端数据管理，选择要启用的版本并恢复，可解决对应冲突。</p></div>}
      {manager && <div className="cloud-manager">
        <div className="settings-actions">
          <input aria-label="筛选云端文件" placeholder="按路径筛选策略、日期或配置" value={filter} onChange={e => setFilter(e.target.value)} />
          <label><input type="checkbox" checked={showHistory} onChange={e => setShowHistory(e.target.checked)} /> 显示历史版本</label>
          <button className="settings-secondary-btn" type="button" disabled={busy} onClick={() => setManager(false)}>收起</button>
        </div>
        <p className="settings-section-note">恢复前备份本地文件；冲突时不会自动覆盖。删除只影响云端选定版本。</p>
        {visible.length === 0 ? <p>暂无匹配数据。首次配置后点击“同步云端”。</p> : <div className="cloud-item-list">{visible.map(item => <article className="cloud-item" key={`${item.path}:${item.version.id}`}>
          <div className="cloud-item-main"><strong>{cloudPathLabel(item.path)}</strong><span>{item.path}</span><span>{item.conflict && item.isHead ? '待解决冲突 · ' : item.isHead ? '当前版本 · ' : '历史版本 · '}{new Date(item.version.createdAt).toLocaleString()}</span><span>原始 {bytes(item.version.size)} · 单文件压缩 {bytes(item.version.compressedSize)} · {item.version.hash.slice(0, 12)}</span></div>
          <div className="settings-actions"><button className="settings-secondary-btn" type="button" disabled={busy || disabled} onClick={() => { setSelected(item); setAction('restore') }}>恢复 / 启用</button><button className="settings-danger-btn" type="button" disabled={busy || disabled} onClick={() => { setSelected(item); setAction('delete') }}>删除云端版本</button></div>
        </article>)}</div>}
      </div>}
      {selected && action && <div className="settings-modal-backdrop cloud-modal-overlay"><section className="settings-modal settings-modal-narrow" role="dialog" aria-modal="true" aria-label={action === 'restore' ? '恢复云端版本' : '删除云端版本'}>
        <h3>{action === 'restore' ? '恢复并启用这个版本' : '删除这个云端版本'}</h3>
        <p className="cloud-path">{cloudPathLabel(selected.path)} · {selected.path}</p><p>{new Date(selected.version.createdAt).toLocaleString()} · {selected.version.hash.slice(0, 12)}</p>
        <p>{action === 'restore' ? '会先保留当前本地内容，再启用选定版本；该文件的冲突分支将合并，历史版本仍可恢复。' : '仅删除这个云端版本，本地数据保留。删除后相同的已同步本地内容不会自动上传；删除的版本将无法再从云端恢复。'}</p>
        <div className="settings-actions"><button className="settings-secondary-btn" type="button" disabled={busy} onClick={() => { setSelected(null); setAction(null) }}>取消</button><button className={action === 'delete' ? 'settings-danger-btn' : 'settings-primary-btn'} type="button" disabled={busy || disabled} onClick={() => void operate(action, selected)}>{busy ? '处理中…' : action === 'delete' ? '确认删除' : '确认恢复'}</button></div>
      </section></div>}
      {settings && <CloudSyncSettingsModal onClose={() => setSettings(false)} />}
    </section>
  )
}
