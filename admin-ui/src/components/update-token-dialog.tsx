import { useState } from 'react'
import { toast } from 'sonner'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { useUpdateRefreshToken } from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'
import type { CredentialStatusItem } from '@/types/api'

interface UpdateTokenDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  credential: CredentialStatusItem
}

// 从 KAM JSON 或纯字符串中提取 refreshToken
function extractRefreshToken(input: string): string {
  const trimmed = input.trim()
  if (!trimmed) return ''

  // 尝试解析为 JSON
  try {
    const parsed = JSON.parse(trimmed)

    // 单个 KAM 账号对象（新格式）：{ refreshToken: "..." }
    if (typeof parsed.refreshToken === 'string') {
      return parsed.refreshToken.trim()
    }

    // 单个 KAM 账号对象（旧格式）：{ credentials: { refreshToken: "..." } }
    if (parsed.credentials && typeof parsed.credentials.refreshToken === 'string') {
      return parsed.credentials.refreshToken.trim()
    }

    // 数组格式，取第一个
    if (Array.isArray(parsed) && parsed.length > 0) {
      const first = parsed[0]
      if (typeof first.refreshToken === 'string') return first.refreshToken.trim()
      if (first.credentials && typeof first.credentials.refreshToken === 'string') {
        return first.credentials.refreshToken.trim()
      }
    }

    return ''
  } catch {
    // 不是 JSON，直接当作 refreshToken 字符串使用
    return trimmed
  }
}

export function UpdateTokenDialog({ open, onOpenChange, credential }: UpdateTokenDialogProps) {
  const [input, setInput] = useState('')
  const { mutate, isPending } = useUpdateRefreshToken()

  const extractedToken = extractRefreshToken(input)
  const isValid = extractedToken.length >= 100 && !extractedToken.includes('...')

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()

    if (!isValid) {
      toast.error('refreshToken 无效或已被截断')
      return
    }

    mutate(
      { id: credential.id, req: { refreshToken: extractedToken } },
      {
        onSuccess: (res) => {
          toast.success(res.message)
          setInput('')
          onOpenChange(false)
        },
        onError: (error: unknown) => {
          toast.error(`更新失败: ${extractErrorMessage(error)}`)
        },
      }
    )
  }

  const handleClose = () => {
    if (!isPending) {
      setInput('')
      onOpenChange(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>更新 refreshToken</DialogTitle>
          <DialogDescription>
            为已禁用的凭据 #{credential.id}（{credential.email || '未知邮箱'}）更新 refreshToken。
            更新后凭据仍保持禁用状态，请手动启用。
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit}>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">
                粘贴 KAM 导出 JSON 或直接粘贴 refreshToken 字符串
              </label>
              <textarea
                placeholder={'支持以下格式：\n\n1. 直接粘贴 refreshToken 字符串\n\n2. KAM 导出的单账号 JSON：\n{\n  "email": "...",\n  "refreshToken": "aor...",\n  "authMethod": "social"\n}'}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                disabled={isPending}
                className="flex min-h-[160px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 font-mono"
              />
            </div>

            {/* 解析结果预览 */}
            {input.trim() && (
              <div className={`text-sm rounded-md p-3 ${isValid ? 'bg-green-50 dark:bg-green-950 text-green-700 dark:text-green-300' : 'bg-red-50 dark:bg-red-950 text-red-700 dark:text-red-300'}`}>
                {isValid ? (
                  <>
                    已识别 refreshToken（{extractedToken.length} 字符）：
                    <span className="font-mono text-xs block mt-1 opacity-75">
                      {extractedToken.slice(0, 20)}...{extractedToken.slice(-10)}
                    </span>
                  </>
                ) : (
                  extractedToken.length > 0
                    ? `Token 无效：长度 ${extractedToken.length} 字符（需要 ≥100 字符）`
                    : '无法识别 refreshToken，请检查格式'
                )}
              </div>
            )}
          </div>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={handleClose} disabled={isPending}>
              取消
            </Button>
            <Button type="submit" disabled={isPending || !isValid}>
              {isPending ? '更新中...' : '确认更新'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
