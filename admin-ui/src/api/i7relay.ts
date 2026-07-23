import axios from 'axios'
import { storage } from '@/lib/storage'

const api = axios.create({
  baseURL: '/api/admin',
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
})

api.interceptors.request.use((config) => {
  const apiKey = storage.getApiKey()
  if (apiKey) config.headers['x-api-key'] = apiKey
  return config
})

/** i7relay 配置(脱敏:apiKey 掩码、webhookSecret 只回是否已设)。 */
export interface I7relayConfig {
  enabled: boolean
  baseUrl: string
  purchaseCount: number
  pollIntervalSecs: number
  restockThreshold: number
  verifyOnImport: boolean
  deadKeyAction: string
  cooldownSecs: number
  apiKeySet: boolean
  apiKeyMasked: string
  webhookSecretSet: boolean
}

/** 保存请求(apiKey/webhookSecret 空=保留原值)。 */
export interface SetI7relayConfigRequest {
  enabled?: boolean
  baseUrl?: string
  purchaseCount?: number
  pollIntervalSecs?: number
  restockThreshold?: number
  verifyOnImport?: boolean
  deadKeyAction?: string
  cooldownSecs?: number
  apiKey?: string
  clearApiKey?: boolean
  webhookSecret?: string
}

export interface QuotaInfo {
  name: string
  remaining: number
  max_quota: number
  used_quota: number
  /** 供应商当前注册的 webhook URL(空=未注册)。 */
  webhook_url?: string
}

export interface RestockResult {
  imported: number
  duplicate: number
  failed: number
  /** -1 = 未知(purchase 未返回配额/失败),前端不据此覆盖显示。 */
  remainingQuota: number
  /** 失败原因(如"暂无可用 Key");成功为 null。 */
  error?: string | null
}

export interface RestockRecord {
  at: string
  trigger: string
  requested: number
  imported: number
  duplicate: number
  failed: number
  disabled: number
  remaining_quota: number
  key_prefixes: string[]
  error?: string
}

export interface I7relayStatus {
  enabled: boolean
  baseUrl?: string
  purchaseCount?: number
  restockThreshold?: number
  pollIntervalSecs?: number
  deadKeyAction?: string
  poolI7relayTotal?: number
  poolI7relayActive?: number
  recentRestocks?: RestockRecord[]
}

export async function getI7relayConfig(): Promise<I7relayConfig> {
  const { data } = await api.get<I7relayConfig>('/config/i7relay')
  return data
}

export async function setI7relayConfig(req: SetI7relayConfigRequest): Promise<I7relayConfig> {
  const { data } = await api.put<I7relayConfig>('/config/i7relay', req)
  return data
}

export async function restockNow(): Promise<RestockResult> {
  const { data } = await api.post<RestockResult>('/i7relay/restock-now')
  return data
}

export async function getI7relayQuota(): Promise<QuotaInfo> {
  const { data } = await api.get<QuotaInfo>('/i7relay/quota')
  return data
}

export async function registerWebhook(webhookUrl: string): Promise<{ ok: boolean; webhookUrl: string }> {
  const { data } = await api.post('/i7relay/register-webhook', { webhookUrl })
  return data
}

export async function getI7relayStatus(): Promise<I7relayStatus> {
  const { data } = await api.get<I7relayStatus>('/i7relay/status')
  return data
}

/** 单个 key 的提取记录。 */
export interface KeyExtractRecord {
  at: string
  key_prefix: string
  trigger: string
  import_status: string
  valid?: boolean | null
  credential_id?: number | null
}

export async function getI7relayExtracts(limit = 100): Promise<KeyExtractRecord[]> {
  const { data } = await api.get<{ extracts: KeyExtractRecord[] }>('/i7relay/extracts', {
    params: { limit },
  })
  return data.extracts ?? []
}

/** 本轮最大可提取数量。 */
export async function getI7relayStock(): Promise<number> {
  const { data } = await api.get<{ max: number }>('/i7relay/stock')
  return data.max ?? 0
}

/** 供应商系统状态(原样透传)。 */
export interface I7relaySystemStatus {
  keys_active?: number
  keys_dead?: number
  keys_stock?: number
  keys_total?: number
  generating?: boolean
  auto_check?: boolean
  auto_generate?: boolean
  uptime_seconds?: number
  [k: string]: unknown
}

export async function getI7relaySystemStatus(): Promise<I7relaySystemStatus> {
  const { data } = await api.get<I7relaySystemStatus>('/i7relay/system-status')
  return data
}

/** 让供应商向我方 webhook 推一条测试消息。 */
export async function testI7relayWebhook(): Promise<{ ok: boolean }> {
  const { data } = await api.post('/i7relay/test-webhook')
  return data
}

