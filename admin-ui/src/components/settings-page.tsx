import { useState } from 'react'
import { toast } from 'sonner'
import { DatabaseZap, Filter, Shuffle, ShieldAlert, ScrollText, Waypoints } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import {
  useRuntimeGovernanceConfig,
  useSetRuntimeGovernanceConfig,
  usePromptFilterDefaults,
  useSetPromptFilterDefaults,
  useAccountThrottleConfig,
  useSetAccountThrottleConfig,
  useLoadBalancingMode,
  useSetLoadBalancingMode,
  useLogGovernanceConfig,
  useSetLogGovernanceConfig,
  useGlobalProxy,
  useSetGlobalProxy,
  useEndpointRoutingConfig,
  useSetEndpointRoutingConfig,
} from '@/hooks/use-credentials'
import { ModelMappingPanel } from '@/components/model-mapping-panel'
import { extractErrorMessage } from '@/lib/utils'

// 区块外壳：图标标题 + 描述 + 内容
function SettingSection({
  icon,
  title,
  desc,
  children,
}: {
  icon: React.ReactNode
  title: string
  desc: string
  children: React.ReactNode
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          {icon}
          {title}
        </CardTitle>
        <CardDescription>{desc}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">{children}</CardContent>
    </Card>
  )
}

// 一行开关：标题 + 说明 + Switch
function ToggleRow({
  label,
  desc,
  checked,
  disabled,
  onChange,
}: {
  label: string
  desc: string
  checked: boolean
  disabled?: boolean
  onChange: (v: boolean) => void
}) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-md bg-secondary/40 px-3 py-2.5">
      <div className="text-sm">
        <div className="font-medium text-foreground">{label}</div>
        <div className="leading-snug text-muted-foreground">{desc}</div>
      </div>
      <Switch checked={checked} disabled={disabled} onCheckedChange={onChange} />
    </div>
  )
}

// PLACEHOLDER_SECTIONS

// Kiro 端点路由（首选端点 + fallback 开关）
function EndpointRoutingSection() {
  const { data: cfg, isLoading } = useEndpointRoutingConfig()
  const { mutate, isPending } = useSetEndpointRoutingConfig()

  const save = (patch: Record<string, unknown>, ok: string) =>
    mutate(patch, {
      onSuccess: () => toast.success(ok),
      onError: (err) => toast.error('保存失败：' + extractErrorMessage(err)),
    })

  // 端点名 → 中文标签（未知名回退原样）
  const label = (name: string): string => {
    switch (name) {
      case 'auto':
        return '自动'
      case 'kiro':
        return 'Kiro (ide 别名)'
      case 'ide':
        return 'IDE'
      case 'cli':
        return 'CLI'
      case 'codewhisperer':
        return 'CodeWhisperer'
      case 'amazonq':
        return 'Amazon Q'
      case 'runtime':
        return 'Runtime (kiro.dev)'
      default:
        return name
    }
  }

  const current = cfg?.preferredEndpoint ?? null
  const available = cfg?.availableEndpoints ?? []

  return (
    <SettingSection
      icon={<Waypoints className="h-4 w-4 text-teal-500" />}
      title="Kiro 端点路由"
      desc="首选上游端点与失败回退。auto 依次尝试 ide → codewhisperer → amazonq → runtime；凭据级 endpoint 优先级最高。运行时生效并持久化。"
    >
      <div>
        <div className="mb-1.5 text-sm font-medium">
          首选端点（当前 {isLoading ? '…' : current ? label(current) : '默认 ' + (cfg ? label(cfg.defaultEndpoint) : '')}）
        </div>
        <div className="mb-2 text-[11px] leading-snug text-muted-foreground">
          fallback 开启时，先用首选端点，失败再按顺序尝试其余端点。cli 端点不参与回退。留空回退到默认 / 凭据级端点。
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          {available.map((name) => (
            <Button
              key={name}
              size="sm"
              variant={current === name ? 'default' : 'outline'}
              className="h-8 text-xs"
              disabled={isPending || current === name}
              onClick={() => save({ preferredEndpoint: name }, `首选端点已切换到「${label(name)}」`)}
            >
              {label(name)}
            </Button>
          ))}
          <Button
            size="sm"
            variant={current === null ? 'default' : 'ghost'}
            className="h-8 text-xs"
            disabled={isPending || current === null}
            onClick={() => save({ preferredEndpoint: '' }, '已清除首选端点（回退默认）')}
          >
            清除
          </Button>
        </div>
      </div>
      <ToggleRow
        label={cfg?.endpointFallback ? '端点回退：已启用' : '端点回退：已关闭'}
        desc="首选端点失败时，自动在同一凭据上尝试其余兼容端点。关闭则只用首选端点。"
        checked={cfg?.endpointFallback ?? true}
        disabled={isPending || isLoading}
        onChange={(v) => save({ endpointFallback: v }, v ? '已开启端点回退' : '已关闭端点回退')}
      />
    </SettingSection>
  )
}

// 指纹计量命中率百分比（hits /(hits+misses)），无样本时显示 —。
function hitRatePct(s: { hits: number; misses: number }): string {
  const total = s.hits + s.misses
  if (total <= 0) return '—'
  return `${((s.hits / total) * 100).toFixed(1)}%`
}

// 缓存 / 配额治理
function CacheQuotaSection() {
  const { data: cfg, isLoading } = useRuntimeGovernanceConfig()
  const { mutate, isPending } = useSetRuntimeGovernanceConfig()
  const [threshold, setThreshold] = useState('')
  const [ttl, setTtl] = useState('')
  const [meterTtl, setMeterTtl] = useState('')
  const [maxRatio, setMaxRatio] = useState('')
  const [minTokens, setMinTokens] = useState('')
  const [minTokensOpus, setMinTokensOpus] = useState('')
  const [maxEntries, setMaxEntries] = useState('')

  const save = (patch: Record<string, unknown>, ok: string) =>
    mutate(patch, {
      onSuccess: () => toast.success(ok),
      onError: (err) => toast.error('保存失败：' + extractErrorMessage(err)),
    })

  const num =
    (raw: string, min: number, max: number, parse: (s: string) => number, field: string, ok: string, reset: () => void) =>
    (e: React.FormEvent) => {
      e.preventDefault()
      const n = parse(raw)
      if (isNaN(n) || n < min || n > max) {
        toast.error(`需在 ${min}..=${max}`)
        return
      }
      save({ [field]: n }, ok)
      reset()
    }

  return (
    <SettingSection
      icon={<DatabaseZap className="h-4 w-4 text-sky-500" />}
      title="缓存 / 配额治理"
      desc="配额自动禁用阈值、全局响应缓存默认、Prompt cache 计量。均运行时生效并持久化。"
    >
      <div>
        <div className="mb-1.5 text-sm font-medium">
          配额自动禁用阈值（当前 {cfg ? `${cfg.quotaDisableThreshold}%` : '—'}）
        </div>
        <div className="mb-2 text-[11px] leading-snug text-muted-foreground">
          账号用量达此百分比自动禁用，回落后自动恢复；设为 100 则关闭。
        </div>
        <form onSubmit={num(threshold, 1, 100, parseFloat, 'quotaDisableThreshold', '配额阈值已更新', () => setThreshold(''))} className="flex items-center gap-1.5">
          <Input type="number" min={1} max={100} step="0.1" placeholder="百分比" value={threshold} onChange={(e) => setThreshold(e.target.value)} disabled={isPending} className="h-8 max-w-[160px] text-xs" />
          <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={isPending || !threshold.trim()}>保存</Button>
        </form>
      </div>

      <ToggleRow
        label={cfg?.responseCacheEnabled ? '响应缓存：已启用' : '响应缓存：已关闭'}
        desc={cfg?.responseCacheEnabled ? '相同请求命中即回放、跳过上游' : '全局默认关闭，可被各 Key 单独覆盖'}
        checked={cfg?.responseCacheEnabled ?? false}
        disabled={isLoading || isPending}
        onChange={(v) => save({ responseCacheEnabled: v }, v ? '已开启响应缓存' : '已关闭响应缓存')}
      />

      <div>
        <div className="mb-1.5 text-sm font-medium">响应缓存 TTL 秒（当前 {cfg?.responseCacheTtlSecs ?? '—'}）</div>
        <form onSubmit={num(ttl, 1, 86400, (s) => parseInt(s, 10), 'responseCacheTtlSecs', '缓存 TTL 已更新', () => setTtl(''))} className="flex items-center gap-1.5">
          <Input type="number" min={1} max={86400} placeholder="秒" value={ttl} onChange={(e) => setTtl(e.target.value)} disabled={isPending} className="h-8 max-w-[160px] text-xs" />
          <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={isPending || !ttl.trim()}>保存</Button>
        </form>
      </div>

      <div className="rounded-md border border-pink-200/60 bg-pink-50/30 p-2.5 dark:border-pink-900/40 dark:bg-pink-950/10">
        <div className="mb-2 text-sm font-medium text-pink-600">
          Prompt cache 指纹计量（合成给下游的 token 计量，不缓存真实响应）
        </div>
        <div className="mb-2.5 text-[11px] leading-snug text-muted-foreground">
          命中按<strong>内容指纹物理匹配</strong>（对齐 Kiro-Go）：相同前缀无论哪个会话/账号都命中。
          下面 5 个参数控制命中率——TTL 越长、阈值越低、容量越大、上限越高 → 命中率越高。
          {cfg?.cacheStats && (
            <span className="mt-1 block text-pink-600/80">
              实时统计：命中 {cfg.cacheStats.hits} / 未命中 {cfg.cacheStats.misses}
              （命中率 {hitRatePct(cfg.cacheStats)}）· 表内 {cfg.cacheStats.entries}/{cfg.cacheStats.capacity}
              · 淘汰 {cfg.cacheStats.evictions} · 过期 {cfg.cacheStats.expirations}
            </span>
          )}
        </div>

        <div className="space-y-2.5">
          <div>
            <div className="mb-1 text-xs font-medium">断点默认 TTL 秒（当前 {cfg?.cacheMeterTtlSecs ?? '—'}）</div>
            <div className="mb-1 text-[11px] leading-snug text-muted-foreground">
              断点无显式 cache_control.ttl 时用此值。越长 → 缓存前缀留存越久 → 跨请求命中率↑。默认 300。
            </div>
            <form onSubmit={num(meterTtl, 1, 86400, (s) => parseInt(s, 10), 'cacheMeterTtlSecs', '断点 TTL 已更新', () => setMeterTtl(''))} className="flex items-center gap-1.5">
              <Input type="number" min={1} max={86400} placeholder="秒" value={meterTtl} onChange={(e) => setMeterTtl(e.target.value)} disabled={isPending} className="h-8 max-w-[160px] text-xs" />
              <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={isPending || !meterTtl.trim()}>保存</Button>
            </form>
          </div>

          <div>
            <div className="mb-1 text-xs font-medium">单请求可命中上限（当前 {cfg?.cacheMaxRatio ?? '—'}）</div>
            <div className="mb-1 text-[11px] leading-snug text-muted-foreground">
              单请求可命中占总 input 的上限 ∈ [0.5,1.0]，保证最新内容本轮不全命中。调大 → 命中率更激进。默认 0.85。
            </div>
            <form onSubmit={num(maxRatio, 0.5, 1, parseFloat, 'cacheMaxRatio', '命中上限已更新', () => setMaxRatio(''))} className="flex items-center gap-1.5">
              <Input type="number" min={0.5} max={1} step={0.01} placeholder="0.5 ~ 1.0" value={maxRatio} onChange={(e) => setMaxRatio(e.target.value)} disabled={isPending} className="h-8 max-w-[160px] text-xs" />
              <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={isPending || !maxRatio.trim()}>保存</Button>
            </form>
          </div>

          <div>
            <div className="mb-1 text-xs font-medium">最小可缓存 token（非 opus，当前 {cfg?.cacheMinTokens ?? '—'}）</div>
            <div className="mb-1 text-[11px] leading-snug text-muted-foreground">
              低于此 token 的前缀不进缓存。调小 → 更多小块可缓存 → 命中率↑。默认 1024。
            </div>
            <form onSubmit={num(minTokens, 0, 1000000, (s) => parseInt(s, 10), 'cacheMinTokens', '最小 token 已更新', () => setMinTokens(''))} className="flex items-center gap-1.5">
              <Input type="number" min={0} placeholder="token" value={minTokens} onChange={(e) => setMinTokens(e.target.value)} disabled={isPending} className="h-8 max-w-[160px] text-xs" />
              <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={isPending || !minTokens.trim()}>保存</Button>
            </form>
          </div>

          <div>
            <div className="mb-1 text-xs font-medium">最小可缓存 token（opus，当前 {cfg?.cacheMinTokensOpus ?? '—'}）</div>
            <div className="mb-1 text-[11px] leading-snug text-muted-foreground">
              opus 家族缓存门槛更高。默认 4096。
            </div>
            <form onSubmit={num(minTokensOpus, 0, 1000000, (s) => parseInt(s, 10), 'cacheMinTokensOpus', 'opus 最小 token 已更新', () => setMinTokensOpus(''))} className="flex items-center gap-1.5">
              <Input type="number" min={0} placeholder="token" value={minTokensOpus} onChange={(e) => setMinTokensOpus(e.target.value)} disabled={isPending} className="h-8 max-w-[160px] text-xs" />
              <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={isPending || !minTokensOpus.trim()}>保存</Button>
            </form>
          </div>

          <div>
            <div className="mb-1 text-xs font-medium">全局 LRU 容量上限（当前 {cfg?.cacheMaxEntries ?? '—'}）</div>
            <div className="mb-1 text-[11px] leading-snug text-muted-foreground">
              指纹表最多存多少前缀，超出淘汰最久未用。调大 → 容纳更多前缀 → 高并发命中率↑（占内存更多）。默认 20000，下限 100。
            </div>
            <form onSubmit={num(maxEntries, 100, 100000000, (s) => parseInt(s, 10), 'cacheMaxEntries', 'LRU 容量已更新', () => setMaxEntries(''))} className="flex items-center gap-1.5">
              <Input type="number" min={100} placeholder="条数" value={maxEntries} onChange={(e) => setMaxEntries(e.target.value)} disabled={isPending} className="h-8 max-w-[160px] text-xs" />
              <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={isPending || !maxEntries.trim()}>保存</Button>
            </form>
          </div>
        </div>
      </div>
    </SettingSection>
  )
}

// 提示词过滤默认值（新建 Key 时继承）
function PromptFilterSection() {
  const { data: cfg, isLoading } = usePromptFilterDefaults()
  const { mutate, isPending } = useSetPromptFilterDefaults()
  const save = (patch: Record<string, unknown>, ok: string) =>
    mutate(patch, {
      onSuccess: () => toast.success(ok),
      onError: (err) => toast.error('保存失败：' + extractErrorMessage(err)),
    })
  const busy = isLoading || isPending

  return (
    <SettingSection
      icon={<Filter className="h-4 w-4 text-amber-500" />}
      title="提示词过滤默认值"
      desc="新建客户端 Key 时继承这些开关。现有 Key 不受影响，每把 Key 仍可在编辑里各自覆盖。"
    >
      <ToggleRow
        label="精简 CC 提示词"
        desc="去除 Claude Code 系统提示中的冗余段落"
        checked={cfg?.simplifyCcPrompt ?? false}
        disabled={busy}
        onChange={(v) => save({ simplifyCcPrompt: v }, '已更新默认值')}
      />
      <ToggleRow
        label="去除边界标记"
        desc="剥离请求里的边界/分隔标记噪声"
        checked={cfg?.stripBoundaryMarkers ?? false}
        disabled={busy}
        onChange={(v) => save({ stripBoundaryMarkers: v }, '已更新默认值')}
      />
      <ToggleRow
        label="去除环境噪声"
        desc="剥离环境信息等与任务无关的上下文"
        checked={cfg?.stripEnvNoise ?? false}
        disabled={busy}
        onChange={(v) => save({ stripEnvNoise: v }, '已更新默认值')}
      />
    </SettingSection>
  )
}

// PLACEHOLDER_SECTIONS2

const COOLDOWN_PRESETS = [60, 300, 900, 1800]

// 账号风控 / 负载均衡
function ThrottleLbSection() {
  const { data: throttle, isLoading: tl } = useAccountThrottleConfig()
  const setThrottle = useSetAccountThrottleConfig()
  const { data: lb, isLoading: ll } = useLoadBalancingMode()
  const setLb = useSetLoadBalancingMode()
  const [cooldown, setCooldown] = useState('')

  const saveThrottle = (patch: Record<string, unknown>, ok: string) =>
    setThrottle.mutate(patch, {
      onSuccess: () => toast.success(ok),
      onError: (err) => toast.error('保存失败：' + extractErrorMessage(err)),
    })

  const submitCooldown = (secs: number) => {
    if (isNaN(secs) || secs < 1 || secs > 86400) {
      toast.error('冷却秒数需在 1..=86400')
      return
    }
    saveThrottle({ cooldownSecs: secs }, '冷却时长已更新')
    setCooldown('')
  }

  const mode = lb?.mode ?? 'priority'

  return (
    <SettingSection
      icon={<ShieldAlert className="h-4 w-4 text-red-500" />}
      title="账号风控 / 负载均衡"
      desc="账号级风控故障转移与冷却时长、上游凭据负载均衡模式。"
    >
      <ToggleRow
        label="风控故障转移"
        desc="账号被风控（429/限流）时自动切换到其他可用账号"
        checked={throttle?.failover ?? false}
        disabled={tl || setThrottle.isPending}
        onChange={(v) => saveThrottle({ failover: v }, v ? '已开启故障转移' : '已关闭故障转移')}
      />

      <div>
        <div className="mb-1.5 text-sm font-medium">风控冷却时长（当前 {throttle ? `${throttle.cooldownSecs}s` : '—'}）</div>
        <div className="mb-2 text-[11px] leading-snug text-muted-foreground">被风控的账号在冷却期内不参与调度。</div>
        <div className="flex flex-wrap items-center gap-1.5">
          {COOLDOWN_PRESETS.map((s) => (
            <Button
              key={s}
              size="sm"
              variant={throttle?.cooldownSecs === s ? 'default' : 'outline'}
              className="h-8 text-xs"
              disabled={setThrottle.isPending}
              onClick={() => submitCooldown(s)}
            >
              {s >= 60 ? `${s / 60} 分` : `${s}s`}
            </Button>
          ))}
          <form
            onSubmit={(e) => {
              e.preventDefault()
              submitCooldown(parseInt(cooldown, 10))
            }}
            className="flex items-center gap-1.5"
          >
            <Input type="number" min={1} max={86400} placeholder="自定义秒" value={cooldown} onChange={(e) => setCooldown(e.target.value)} disabled={setThrottle.isPending} className="h-8 max-w-[120px] text-xs" />
            <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={setThrottle.isPending || !cooldown.trim()}>保存</Button>
          </form>
        </div>
      </div>

      <div>
        <div className="mb-1.5 text-sm font-medium">负载均衡模式（当前 {mode === 'priority' ? '优先级' : '均衡负载'}）</div>
        <div className="mb-2 text-[11px] leading-snug text-muted-foreground">
          优先级：按优先级顺序用账号，高优先用尽再降级。均衡负载：在可用账号间轮询分摊。
        </div>
        <div className="flex items-center gap-1.5">
          {(['priority', 'balanced'] as const).map((m) => (
            <Button
              key={m}
              size="sm"
              variant={mode === m ? 'default' : 'outline'}
              className="h-8 text-xs"
              disabled={ll || setLb.isPending}
              onClick={() =>
                setLb.mutate(m, {
                  onSuccess: () => toast.success(`已切换到${m === 'priority' ? '优先级模式' : '均衡负载模式'}`),
                  onError: (err) => toast.error('切换失败：' + extractErrorMessage(err)),
                })
              }
            >
              {m === 'priority' ? '优先级' : '均衡负载'}
            </Button>
          ))}
        </div>
      </div>
    </SettingSection>
  )
}

// 日志治理 / 全局代理
function LogProxySection() {
  const { data: log, isLoading: gl } = useLogGovernanceConfig()
  const setLog = useSetLogGovernanceConfig()
  const { data: proxy, isLoading: pl } = useGlobalProxy()
  const setProxy = useSetGlobalProxy()
  const [traceDays, setTraceDays] = useState('')
  const [usageDays, setUsageDays] = useState('')
  const [proxyUrl, setProxyUrl] = useState('')

  const saveLog = (patch: Record<string, unknown>, ok: string) =>
    setLog.mutate(patch, {
      onSuccess: () => toast.success(ok),
      onError: (err) => toast.error('保存失败：' + extractErrorMessage(err)),
    })

  const submitDays = (raw: string, field: string, ok: string, reset: () => void) => (e: React.FormEvent) => {
    e.preventDefault()
    const n = parseInt(raw, 10)
    if (isNaN(n) || n < 0 || n > 3650) {
      toast.error('保留天数需在 0..=3650')
      return
    }
    saveLog({ [field]: n }, ok)
    reset()
  }

  return (
    <SettingSection
      icon={<ScrollText className="h-4 w-4 text-violet-500" />}
      title="日志治理 / 全局代理"
      desc="请求日志（trace）与用量日志保留期、全局出站代理。"
    >
      <ToggleRow
        label={log?.traceEnabled ? '请求日志：已启用' : '请求日志：已关闭'}
        desc="记录每次请求的链路信息，用于排障与统计"
        checked={log?.traceEnabled ?? false}
        disabled={gl || setLog.isPending}
        onChange={(v) => saveLog({ traceEnabled: v }, v ? '已开启请求日志' : '已关闭请求日志')}
      />

      <div>
        <div className="mb-1.5 text-sm font-medium">请求日志保留天数（当前 {log?.traceRetentionDays ?? '—'}）</div>
        <form onSubmit={submitDays(traceDays, 'traceRetentionDays', '请求日志保留期已更新', () => setTraceDays(''))} className="flex items-center gap-1.5">
          <Input type="number" min={0} max={3650} placeholder="天" value={traceDays} onChange={(e) => setTraceDays(e.target.value)} disabled={setLog.isPending} className="h-8 max-w-[160px] text-xs" />
          <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={setLog.isPending || !traceDays.trim()}>保存</Button>
        </form>
      </div>

      <div>
        <div className="mb-1.5 text-sm font-medium">用量日志保留天数（当前 {log?.usageLogRetentionDays ?? '—'}）</div>
        <form onSubmit={submitDays(usageDays, 'usageLogRetentionDays', '用量日志保留期已更新', () => setUsageDays(''))} className="flex items-center gap-1.5">
          <Input type="number" min={0} max={3650} placeholder="天" value={usageDays} onChange={(e) => setUsageDays(e.target.value)} disabled={setLog.isPending} className="h-8 max-w-[160px] text-xs" />
          <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={setLog.isPending || !usageDays.trim()}>保存</Button>
        </form>
      </div>

      <div>
        <div className="mb-1.5 text-sm font-medium">
          全局出站代理（当前 {pl ? '…' : proxy?.proxyUrl ? proxy.proxyUrl : '未设置'}）
        </div>
        <div className="mb-2 text-[11px] leading-snug text-muted-foreground">
          所有上游请求经此代理出站（未单独指定代理的凭据）。留空并保存即清除。
        </div>
        <form
          onSubmit={(e) => {
            e.preventDefault()
            const url = proxyUrl.trim()
            setProxy.mutate(
              { proxyUrl: url === '' ? null : url },
              {
                onSuccess: () => {
                  toast.success(url === '' ? '已清除全局代理' : '全局代理已更新')
                  setProxyUrl('')
                },
                onError: (err) => toast.error('保存失败：' + extractErrorMessage(err)),
              },
            )
          }}
          className="flex items-center gap-1.5"
        >
          <Input placeholder="http://host:port（留空清除）" value={proxyUrl} onChange={(e) => setProxyUrl(e.target.value)} disabled={setProxy.isPending} className="h-8 text-xs" />
          <Button type="submit" size="sm" variant="outline" className="h-8 text-xs" disabled={setProxy.isPending}>保存</Button>
        </form>
      </div>
    </SettingSection>
  )
}

export function SettingsPage() {
  return (
    <div>
      <div className="mb-6">
        <h1 className="text-[28px] font-semibold leading-tight tracking-tight">设置</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          全局运行时设置，均即时生效并持久化到 config.json。per-key 设置仍在各自的 Key 编辑里。
        </p>
      </div>
      <div className="grid gap-5 lg:grid-cols-2">
        <EndpointRoutingSection />
        <CacheQuotaSection />
        <PromptFilterSection />
        <SettingSection
          icon={<Shuffle className="h-4 w-4 text-emerald-500" />}
          title="模型映射（OpenAI 端点）"
          desc="客户端请求的模型名按规则映射到目标 Claude 模型；未命中原样透传。全局生效、即时保存。"
        >
          <ModelMappingPanel />
        </SettingSection>
        <ThrottleLbSection />
        <LogProxySection />
      </div>
    </div>
  )
}

export default SettingsPage
