import { CheckIcon, SparklesIcon } from 'lucide-react'

import type { SourceType } from '@/api/types'
import { useSources } from '@/hooks/useSources'
import { cn } from '@/lib/utils'
import { sourceTypeLabels } from '@/lib/enumLabels'
import { Skeleton } from '@/components/ui/skeleton'
import { Tooltip, TooltipContent, TooltipTrigger, TooltipProvider } from '@/components/ui/tooltip'

interface SourcePickerProps {
  useAuto: boolean
  onUseAutoChange: (value: boolean) => void
  selected: SourceType[]
  onSelectedChange: (value: SourceType[]) => void
  error?: string
}

export function SourcePicker({ useAuto, onUseAutoChange, selected, onSelectedChange, error }: SourcePickerProps) {
  const { data: sources, isLoading } = useSources()

  function toggle(source: SourceType) {
    onSelectedChange(
      selected.includes(source) ? selected.filter((s) => s !== source) : [...selected, source],
    )
  }

  return (
    <div className="flex flex-col gap-3">
      <button
        type="button"
        onClick={() => onUseAutoChange(true)}
        className={cn(
          'flex items-center gap-2 rounded-lg border px-4 py-3 text-left text-sm transition-colors',
          useAuto ? 'border-primary bg-accent' : 'border-input hover:bg-accent/50',
        )}
      >
        <SparklesIcon className="size-4 shrink-0 text-primary" />
        <div className="flex flex-col">
          <span className="font-medium">Auto (recommended)</span>
          <span className="text-muted-foreground">Search every available source automatically</span>
        </div>
        {useAuto && <CheckIcon className="ml-auto size-4 shrink-0 text-primary" />}
      </button>

      <button
        type="button"
        onClick={() => onUseAutoChange(!useAuto)}
        className="self-start text-xs text-muted-foreground underline-offset-2 hover:underline"
      >
        {useAuto ? 'Choose specific sources instead' : 'Use Auto instead'}
      </button>

      {!useAuto && (
        <TooltipProvider>
          <div className="flex flex-wrap gap-2">
            {isLoading &&
              Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-9 w-28" />)}

            {/* The NAS is set up under Credentials and is not offered as a job source. */}
            {sources?.filter((source) => source.source_type !== 'nas_share').map((source) => {
              const isSelected = selected.includes(source.source_type)
              const disabled = !source.connector_implemented
              const chip = (
                <button
                  key={source.source_type}
                  type="button"
                  disabled={disabled}
                  onClick={() => toggle(source.source_type)}
                  className={cn(
                    'flex items-center gap-2 rounded-full border px-3 py-1.5 text-sm transition-colors disabled:cursor-not-allowed disabled:opacity-50',
                    isSelected ? 'border-primary bg-accent' : 'border-input hover:bg-accent/50',
                  )}
                >
                  <span
                    className={cn(
                      'size-2 rounded-full',
                      disabled ? 'bg-muted-foreground' : source.has_credential ? 'bg-success' : 'bg-warning',
                    )}
                  />
                  {sourceTypeLabels[source.source_type]}
                </button>
              )
              return (
                <Tooltip key={source.source_type}>
                  <TooltipTrigger asChild>{chip}</TooltipTrigger>
                  <TooltipContent>
                    {disabled
                      ? 'Connector coming soon'
                      : source.has_credential
                        ? 'Credential configured'
                        : 'No credential configured — the connector will still run, with reduced results for sources that require one'}
                  </TooltipContent>
                </Tooltip>
              )
            })}
          </div>
        </TooltipProvider>
      )}

      {error && <p className="text-xs text-destructive">{error}</p>}
    </div>
  )
}
