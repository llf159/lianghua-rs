import { useEffect, useState } from 'react'
import { getCloudConfig, saveCloudConfig, testCloudConnection, type CloudConfig } from '../../../apis/cloudSync'
import '../css/CloudSync.css'

export default function CloudSyncSettingsModal({ onClose }: { onClose: () => void }) {
  const [config, setConfig] = useState<CloudConfig | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  useEffect(() => {
    let cancelled = false
    void getCloudConfig().then(value => { if (!cancelled) setConfig(value) }).catch(e => { if (!cancelled) setError(String(e)) })
    return () => { cancelled = true }
  }, [])
  async function save(test: boolean) {
    if (!config || busy) return
    setBusy(true); setError(''); setNotice('')
    try {
      await saveCloudConfig(config)
      setConfig(await getCloudConfig())
      if (test) await testCloudConnection()
      setNotice(test ? '配置已保存，云端连接及同步锁验证通过。' : '配置已保存。到数据管理页点击“下载云端”接收数据，或点击“双向同步”上传并接收。')
    } catch (e) { setError(String(e)) } finally { setBusy(false) }
  }
  return (
    <div className="settings-modal-backdrop cloud-modal-overlay">
      <section className="settings-modal settings-modal-narrow" role="dialog" aria-modal="true" aria-label="WebDAV 云同步配置">
        <div className="settings-head">
          <h3>WebDAV 云同步配置</h3>
          <button className="settings-secondary-btn" type="button" onClick={onClose} disabled={busy}>关闭</button>
        </div>
        <p className="settings-section-note">策略、自选、指标图表配置、浏览习惯分四包。总榜五个结果日期放在一个数据库快照里，相似榜另一个快照保留最新一天；点击管理页按钮双向同步。</p>
        {config ? <fieldset className="cloud-config-fields" disabled={busy}>
          <label className="settings-field"><span>服务器地址</span><input type="url" value={config.endpoint} onChange={e => setConfig({ ...config, endpoint: e.target.value })} autoCapitalize="none" spellCheck={false} /></label>
          <label className="settings-field"><span>网盘子文件夹</span><input value={config.folder} onChange={e => setConfig({ ...config, folder: e.target.value })} placeholder="lianghua-rs" /></label>
          <label className="settings-field"><span>坚果云账号 / 邮箱</span><input value={config.username} onChange={e => setConfig({ ...config, username: e.target.value })} autoCapitalize="none" autoComplete="username" spellCheck={false} /></label>
          <label className="settings-field"><span>第三方应用密码</span><input type="password" value={config.password} onChange={e => setConfig({ ...config, password: e.target.value })} placeholder={config.hasPassword ? '已保存，留空保持原密码' : '输入坚果云应用密码'} autoComplete="new-password" /></label>
          <p className="settings-section-note">在坚果云“账户信息 → 安全选项 → 第三方应用管理”生成应用密码。凭据仅保存在本机应用目录，不参与同步。</p>
          <a href="https://help.jianguoyun.com/?p=2064" target="_blank" rel="noreferrer">坚果云应用密码设置说明</a>
          <div className="settings-actions">
            <button className="settings-primary-btn" type="button" onClick={() => void save(false)}>保存</button>
            <button className="settings-secondary-btn" type="button" onClick={() => void save(true)}>{busy ? '处理中…' : '保存并测试连接'}</button>
          </div>
        </fieldset> : <p>正在读取配置…</p>}
        {error && <p className="cloud-error" role="alert">{error}</p>}
        {notice && <p className="cloud-notice" role="status">{notice}</p>}
      </section>
    </div>
  )
}
