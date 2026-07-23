import { useEffect, useState } from 'react'
import { toast } from 'sonner'
import { CloudDownload, Download, RefreshCw, Eye, EyeOff, Trash2, Copy, Webhook, CheckCircle2, Circle } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { extractErrorMessage } from '@/lib/utils'
import {
  getI7relayConfig,
  setI7relayConfig,
  restockNow,
  getI7relayQuota,
  getI7relayExtracts,
  registerWebhook,
  getI7relayStock,
  getI7relaySystemStatus,
  testI7relayWebhook,
  type I7relayConfig,
  type QuotaInfo,
  type SetI7relayConfigRequest,
  type KeyExtractRecord,
  type I7relaySystemStatus,
} from '@/api/i7relay'

/** 我方接收回调的固定路径(供应商 POST 到此)。 */
const WEBHOOK_PATH = '/api/admin/webhook/account-refill'

export function AutoRefillPage() {
  const [cfg, setCfg] = useState<I7relayConfig | null>(null)
  const [enabled, setEnabled] = useState(false)
  const [baseUrl, setBaseUrl] = useState('')
  const [purchaseCount, setPurchaseCount] = useState('1')
  const [apiKey, setApiKey] = useState('')
  const [showKey, setShowKey] = useState(false)
  const [clearKey, setClearKey] = useState(false)
  const [webhookSecret, setWebhookSecret] = useState('')
  const [quota, setQuota] = useState<QuotaInfo | null>(null)
  const [extracts, setExtracts] = useState<KeyExtractRecord[]>([])
  const [stockMax, setStockMax] = useState<number | null>(null)
  const [sysStatus, setSysStatus] = useState<I7relaySystemStatus | null>(null)
  const [busy, setBusy] = useState(false)

  const applyCfg = (c: I7relayConfig) => {
    setCfg(c)
    setEnabled(c.enabled)
    setBaseUrl(c.baseUrl)
    setPurchaseCount(String(c.purchaseCount))
    setApiKey('')
    setClearKey(false)
  }

  const loadExtracts = async () => {
    try {
      setExtracts(await getI7relayExtracts(100))
    } catch {
      /* 记录不是关键路径,静默 */
    }
  }

  const load = async () => {
    try {
      applyCfg(await getI7relayConfig())
    } catch (e) {
      toast.error('加载配置失败：' + extractErrorMessage(e))
    }
    loadExtracts()
  }
  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const save = async () => {
    setBusy(true)
    try {
      const req: SetI7relayConfigRequest = {
        enabled,
        baseUrl: baseUrl.trim(),
        purchaseCount: parseInt(purchaseCount, 10) || 1,
      }
      if (clearKey) req.clearApiKey = true
      else if (apiKey.trim()) req.apiKey = apiKey.trim()
      if (webhookSecret.trim()) req.webhookSecret = webhookSecret.trim()
      applyCfg(await setI7relayConfig(req))
      setWebhookSecret('')
      toast.success('配置已保存')
    } catch (e) {
      toast.error('保存失败：' + extractErrorMessage(e))
    } finally {
      setBusy(false)
    }
  }

  // 我方接收地址(供应商 POST 到此)。有 secret 时带 ?token=,让供应商裸 POST 也能鉴权。
  const webhookReceiveUrl = () => {
    const origin = window.location.origin
    const base = `${origin}${WEBHOOK_PATH}`
    return cfg?.webhookSecretSet ? `${base}?token=<你的webhookSecret>` : base
  }

  const copyWebhookUrl = async () => {
    // 复制真实可用地址:已知 secret 明文时不回传,这里给不含 token 的基址 + 提示。
    const url = `${window.location.origin}${WEBHOOK_PATH}`
    try {
      await navigator.clipboard.writeText(url)
      toast.success('已复制接收地址(如设了 secret,记得在末尾加 ?token=你的secret)')
    } catch {
      toast.error('复制失败,请手动选择')
    }
  }

  const doRegisterWebhook = async () => {
    // 注册到供应商:用当前浏览器 origin + 路径(+ 若刚填了 secret 则带 token)。
    const secretForUrl = webhookSecret.trim()
    let url = `${window.location.origin}${WEBHOOK_PATH}`
    if (secretForUrl) url += `?token=${encodeURIComponent(secretForUrl)}`
    else if (cfg?.webhookSecretSet) {
      toast.error('已设 secret 但注册需明文 token:请在"webhookSecret"框重填一次再点注册')
      return
    }
    setBusy(true)
    try {
      await registerWebhook(url)
      toast.success('已注册到供应商(覆盖其原有 webhook)')
      checkQuota() // 刷新 profile.webhook_url 以确认
    } catch (e) {
      toast.error('注册失败：' + extractErrorMessage(e))
    } finally {
      setBusy(false)
    }
  }

  const doRestock = async () => {
    setBusy(true)
    try {
      const r = await restockNow()
      if (r.error) {
        // 供应站侧原因(如"暂无可用 Key"):明确报失败,不动配额显示。
        toast.error('拉取失败：' + r.error)
      } else if (r.imported === 0 && r.duplicate === 0 && r.failed === 0) {
        toast.info('本次未拉到新 Key(供应站暂无可用)')
      } else {
        toast.success(`拉取完成：新增 ${r.imported}，重复 ${r.duplicate}，失败 ${r.failed}`)
      }
      // 仅当 remaining 已知(>=0)才更新显示,-1(未知)绝不覆盖成 0。
      if (r.remainingQuota >= 0) setQuota((q) => (q ? { ...q, remaining: r.remainingQuota } : q))
      loadExtracts() // 拉完刷新提取记录
    } catch (e) {
      toast.error('拉取失败：' + extractErrorMessage(e))
    } finally {
      setBusy(false)
    }
  }

  const checkQuota = async () => {
    setBusy(true)
    try {
      // 并发拉配额 + 本轮可提取 + 供应商系统状态。
      const [q, stock, st] = await Promise.allSettled([
        getI7relayQuota(),
        getI7relayStock(),
        getI7relaySystemStatus(),
      ])
      if (q.status === 'fulfilled') setQuota(q.value)
      if (stock.status === 'fulfilled') setStockMax(stock.value)
      if (st.status === 'fulfilled') setSysStatus(st.value)
      if (q.status === 'rejected') toast.error('查配额失败：' + extractErrorMessage(q.reason))
    } finally {
      setBusy(false)
    }
  }

  const doTestWebhook = async () => {
    setBusy(true)
    try {
      await testI7relayWebhook()
      toast.success('已请求供应商推送测试消息（片刻后应收到 webhook 回调）')
    } catch (e) {
      toast.error('测试 webhook 失败：' + extractErrorMessage(e))
    } finally {
      setBusy(false)
    }
  }

  const count = parseInt(purchaseCount, 10) || 1
  const keyPlaceholder = cfg?.apiKeySet ? '留空则保留已保存的 Key' : 'usr-...'

  return (
    <div className="mx-auto max-w-xl p-4">
      <Card>
        <CardContent className="space-y-6 p-6">
          {/* 标题 + 健康徽章 */}
          <div>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2 text-lg font-semibold">
                <CloudDownload className="h-5 w-5" />
                自动拉取凭证
              </div>
              {enabled ? (
                <span className="flex items-center gap-1 rounded-full bg-emerald-100 px-2.5 py-0.5 text-xs font-medium text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400">
                  <CheckCircle2 className="h-3.5 w-3.5" /> 自动提取已启用
                </span>
              ) : (
                <span className="flex items-center gap-1 rounded-full bg-muted px-2.5 py-0.5 text-xs font-medium text-muted-foreground">
                  <Circle className="h-3.5 w-3.5" /> 未启用
                </span>
              )}
            </div>
            <p className="mt-1 text-sm text-muted-foreground">
              当前请求找不到可用凭证时，从供应站购买 Kiro API Key 并自动加入凭证池。
            </p>
          </div>

          {/* 启用开关 */}
          <div className="flex items-center justify-between rounded-lg border p-3">
            <div>
              <div className="font-semibold">启用自动拉取</div>
              <div className="text-xs text-muted-foreground">拉取失败后 30 秒内不会重复请求供应站</div>
            </div>
            <Switch checked={enabled} onCheckedChange={setEnabled} disabled={busy} />
          </div>

          {/* 供应站 URL */}
          <div className="space-y-1.5">
            <label className="text-sm font-medium">供应站 URL</label>
            <Input
              value={baseUrl}
              onChange={(e) => setBaseUrl(e.target.value)}
              placeholder="https://aws-cui.i7relay.com/"
              disabled={busy}
            />
          </div>

          {/* 供应站 API Key */}
          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">供应站 API Key</label>
              {cfg?.apiKeySet && (
                <span className="rounded bg-emerald-100 px-2 py-0.5 text-xs text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400">
                  已保存
                </span>
              )}
            </div>
            <div className="relative">
              <Input
                type={showKey ? 'text' : 'password'}
                value={clearKey ? '' : apiKey}
                onChange={(e) => {
                  setApiKey(e.target.value)
                  setClearKey(false)
                }}
                placeholder={clearKey ? '（将清除已保存的 Key）' : keyPlaceholder}
                disabled={busy || clearKey}
                className="pr-9"
              />
              <button
                type="button"
                onClick={() => setShowKey((s) => !s)}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                tabIndex={-1}
              >
                {showKey ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
              </button>
            </div>
            {cfg?.apiKeySet && (
              <button
                type="button"
                onClick={() => {
                  setClearKey(true)
                  setApiKey('')
                }}
                className="flex items-center gap-1 text-xs text-muted-foreground hover:text-destructive"
              >
                <Trash2 className="h-3.5 w-3.5" /> 清除已保存的 Key
              </button>
            )}
          </div>

          {/* 单次拉取数量 */}
          <div className="space-y-1.5">
            <label className="text-sm font-medium">单次拉取数量</label>
            <Input
              type="number"
              min={1}
              max={1000}
              value={purchaseCount}
              onChange={(e) => setPurchaseCount(e.target.value)}
              disabled={busy}
            />
          </div>

          {/* Webhook 密钥(URL token) */}
          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">Webhook 密钥(URL token)</label>
              {cfg?.webhookSecretSet && (
                <span className="rounded bg-emerald-100 px-2 py-0.5 text-xs text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400">
                  已保存
                </span>
              )}
            </div>
            <Input
              type="password"
              value={webhookSecret}
              onChange={(e) => setWebhookSecret(e.target.value)}
              placeholder={cfg?.webhookSecretSet ? '留空则保留已保存的密钥' : '建议设置，防他人误触发补货'}
              disabled={busy}
            />
            <p className="text-xs text-muted-foreground">
              供应商回调无鉴权头，故校验地址里的 <code>?token=</code>。留空则不校验（放行）。
            </p>
          </div>

          {/* 我方 Webhook 接收地址(给供应商) */}
          <div className="space-y-2 rounded-lg border p-4">
            <div className="flex items-center gap-1.5 text-sm font-semibold">
              <Webhook className="h-4 w-4" /> 我方接收地址(交给供应商)
            </div>
            <div className="flex items-center gap-2">
              <Input readOnly value={webhookReceiveUrl()} className="font-mono text-xs" />
              <Button variant="outline" size="sm" onClick={copyWebhookUrl} disabled={busy}>
                <Copy className="h-3.5 w-3.5" />
              </Button>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Button variant="outline" size="sm" onClick={doRegisterWebhook} disabled={busy}>
                注册到供应商
              </Button>
              <Button variant="outline" size="sm" onClick={doTestWebhook} disabled={busy}>
                测试 webhook
              </Button>
              <span className="text-xs text-amber-600">注册会覆盖供应商侧原有 webhook</span>
            </div>
            {quota?.webhook_url !== undefined && (
              <p className="break-all text-xs text-muted-foreground">
                供应商当前已注册：<span className="font-mono">{quota.webhook_url || '(未注册)'}</span>
              </p>
            )}
          </div>

          {/* 供应站配额卡片 */}
          <div className="rounded-lg border p-4">
            <div className="mb-3 flex items-center justify-between">
              <div>
                <div className="font-semibold">供应站配额</div>
                {quota && <div className="text-xs text-muted-foreground">{quota.name || '—'}</div>}
              </div>
              <button
                type="button"
                onClick={checkQuota}
                disabled={busy}
                className="text-muted-foreground hover:text-foreground"
                title="刷新配额"
              >
                <RefreshCw className={`h-4 w-4 ${busy ? 'animate-spin' : ''}`} />
              </button>
            </div>
            <div className="grid grid-cols-3 divide-x rounded-md bg-muted/50 py-3 text-center">
              <div>
                <div className="text-xs text-muted-foreground">总配额</div>
                <div className="text-lg font-semibold">{quota ? quota.max_quota : '—'}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">已使用</div>
                <div className="text-lg font-semibold">{quota ? quota.used_quota : '—'}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">剩余</div>
                <div className="text-lg font-semibold text-emerald-600">
                  {quota ? quota.remaining : '—'}
                </div>
              </div>
            </div>
            {stockMax !== null && (
              <p className="mt-2 text-center text-xs text-muted-foreground">
                本轮最大可提取：<span className="font-semibold text-foreground">{stockMax}</span> 个
                {stockMax === 0 && '（供应商暂无可分配给本账号的库存）'}
              </p>
            )}
            <Button variant="outline" className="mt-3 w-full" onClick={doRestock} disabled={busy}>
              <Download className="mr-1.5 h-4 w-4" /> 立即拉取 {count} 个
            </Button>
          </div>

          {/* 供应商系统状态 */}
          {sysStatus && (
            <div className="rounded-lg border p-4">
              <div className="mb-2 text-sm font-semibold">
                供应商系统状态
                {sysStatus.generating && (
                  <span className="ml-2 rounded bg-blue-100 px-1.5 py-0.5 text-xs text-blue-700 dark:bg-blue-900/40 dark:text-blue-400">
                    生成中
                  </span>
                )}
              </div>
              <div className="grid grid-cols-4 gap-2 text-center text-xs">
                <div>
                  <div className="text-muted-foreground">活跃</div>
                  <div className="font-semibold text-emerald-600">{sysStatus.keys_active ?? '—'}</div>
                </div>
                <div>
                  <div className="text-muted-foreground">失效</div>
                  <div className="font-semibold">{sysStatus.keys_dead ?? '—'}</div>
                </div>
                <div>
                  <div className="text-muted-foreground">库存</div>
                  <div className="font-semibold">{sysStatus.keys_stock ?? '—'}</div>
                </div>
                <div>
                  <div className="text-muted-foreground">总计</div>
                  <div className="font-semibold">{sysStatus.keys_total ?? '—'}</div>
                </div>
              </div>
            </div>
          )}

          {/* 提取记录 */}
          <div className="space-y-2 rounded-lg border p-4">
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold">提取记录(最近 {extracts.length} 条)</div>
              <button
                type="button"
                onClick={loadExtracts}
                disabled={busy}
                className="text-muted-foreground hover:text-foreground"
                title="刷新记录"
              >
                <RefreshCw className="h-4 w-4" />
              </button>
            </div>
            {extracts.length === 0 ? (
              <p className="py-2 text-center text-xs text-muted-foreground">暂无提取记录</p>
            ) : (
              <div className="max-h-64 overflow-auto">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-background text-muted-foreground">
                    <tr className="border-b text-left">
                      <th className="py-1.5 pr-2 font-medium">时间</th>
                      <th className="py-1.5 pr-2 font-medium">Key 前缀</th>
                      <th className="py-1.5 pr-2 font-medium">触发</th>
                      <th className="py-1.5 pr-2 font-medium">导入</th>
                      <th className="py-1.5 font-medium">有效</th>
                    </tr>
                  </thead>
                  <tbody>
                    {extracts.map((e, i) => (
                      <tr key={i} className="border-b border-border/50">
                        <td className="py-1.5 pr-2 text-muted-foreground">
                          {new Date(e.at).toLocaleString()}
                        </td>
                        <td className="py-1.5 pr-2 font-mono">{e.key_prefix}</td>
                        <td className="py-1.5 pr-2 text-muted-foreground">
                          {e.trigger.replace('webhook:', '').replace('_', ' ')}
                        </td>
                        <td className="py-1.5 pr-2">
                          <ImportBadge status={e.import_status} />
                        </td>
                        <td className="py-1.5">
                          {e.valid === true ? '✓' : e.valid === false ? '✗' : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* 底部操作 */}
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={load} disabled={busy}>
              取消
            </Button>
            <Button onClick={save} disabled={busy}>
              保存
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

/** 导入状态小徽章。 */
function ImportBadge({ status }: { status: string }) {
  const map: Record<string, string> = {
    imported: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400',
    duplicate: 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-400',
    failed: 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-400',
  }
  const label: Record<string, string> = { imported: '新增', duplicate: '重复', failed: '失败' }
  return (
    <span className={`rounded px-1.5 py-0.5 ${map[status] ?? 'bg-muted text-muted-foreground'}`}>
      {label[status] ?? status}
    </span>
  )
}
