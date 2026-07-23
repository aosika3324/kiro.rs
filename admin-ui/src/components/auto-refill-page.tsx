import { useEffect, useState } from 'react'
import { toast } from 'sonner'
import { CloudDownload, Download, RefreshCw, Eye, EyeOff, Trash2 } from 'lucide-react'
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
  type I7relayConfig,
  type QuotaInfo,
  type SetI7relayConfigRequest,
} from '@/api/i7relay'

export function AutoRefillPage() {
  const [cfg, setCfg] = useState<I7relayConfig | null>(null)
  const [enabled, setEnabled] = useState(false)
  const [baseUrl, setBaseUrl] = useState('')
  const [purchaseCount, setPurchaseCount] = useState('1')
  const [apiKey, setApiKey] = useState('')
  const [showKey, setShowKey] = useState(false)
  const [clearKey, setClearKey] = useState(false)
  const [quota, setQuota] = useState<QuotaInfo | null>(null)
  const [busy, setBusy] = useState(false)

  const applyCfg = (c: I7relayConfig) => {
    setCfg(c)
    setEnabled(c.enabled)
    setBaseUrl(c.baseUrl)
    setPurchaseCount(String(c.purchaseCount))
    setApiKey('')
    setClearKey(false)
  }

  const load = async () => {
    try {
      applyCfg(await getI7relayConfig())
    } catch (e) {
      toast.error('加载配置失败：' + extractErrorMessage(e))
    }
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
      applyCfg(await setI7relayConfig(req))
      toast.success('配置已保存')
    } catch (e) {
      toast.error('保存失败：' + extractErrorMessage(e))
    } finally {
      setBusy(false)
    }
  }

  const doRestock = async () => {
    setBusy(true)
    try {
      const r = await restockNow()
      toast.success(`拉取完成：新增 ${r.imported}，重复 ${r.duplicate}，失败 ${r.failed}`)
      if (r.remainingQuota >= 0) setQuota((q) => (q ? { ...q, remaining: r.remainingQuota } : q))
    } catch (e) {
      toast.error('拉取失败：' + extractErrorMessage(e))
    } finally {
      setBusy(false)
    }
  }

  const checkQuota = async () => {
    setBusy(true)
    try {
      setQuota(await getI7relayQuota())
    } catch (e) {
      toast.error('查配额失败：' + extractErrorMessage(e))
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
          {/* 标题 */}
          <div>
            <div className="flex items-center gap-2 text-lg font-semibold">
              <CloudDownload className="h-5 w-5" />
              自动拉取凭证
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
            <Button variant="outline" className="mt-3 w-full" onClick={doRestock} disabled={busy}>
              <Download className="mr-1.5 h-4 w-4" /> 立即拉取 {count} 个
            </Button>
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
