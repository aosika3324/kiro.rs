import * as React from 'react'
import { cn } from '@/lib/utils'

interface ProgressProps extends React.HTMLAttributes<HTMLDivElement> {
  value?: number
  max?: number
}

const Progress = React.forwardRef<HTMLDivElement, ProgressProps>(
  ({ className, value = 0, max = 100, ...props }, ref) => {
    const percentage = Math.min(Math.max((value / max) * 100, 0), 100)
    const tone =
      percentage > 80
        ? 'bg-gradient-to-r from-rose-500 to-red-500'
        : percentage > 60
        ? 'bg-gradient-to-r from-amber-400 to-orange-500'
        : 'bg-gradient-to-r from-emerald-400 to-emerald-500'

    return (
      <div
        ref={ref}
        data-slot="progress"
        className={cn('relative h-1.5 w-full overflow-hidden rounded-full bg-secondary/80', className)}
        {...props}
      >
        <div
          data-slot="progress-indicator"
          className={cn('h-full transition-all duration-500 ease-apple rounded-full', tone)}
          style={{ width: `${percentage}%` }}
        />
      </div>
    )
  }
)
Progress.displayName = 'Progress'

export { Progress }
