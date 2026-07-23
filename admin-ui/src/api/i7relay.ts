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
}

export interface RestockResult {
  imported: number
  duplicate: number
  failed: number
  remainingQuota: number
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
