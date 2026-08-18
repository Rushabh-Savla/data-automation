import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Card } from '@/components/ui/card'

interface TargetDeadlinePickerProps {
  targetCountEnabled: boolean
  onTargetCountEnabledChange: (value: boolean) => void
  targetCount: number | undefined
  onTargetCountChange: (value: number | undefined) => void

  deadlineEnabled: boolean
  onDeadlineEnabledChange: (value: boolean) => void
  deadline: string | undefined
  onDeadlineChange: (value: string | undefined) => void

  error?: string
  targetCountError?: string
  deadlineError?: string
}

export function TargetDeadlinePicker({
  targetCountEnabled,
  onTargetCountEnabledChange,
  targetCount,
  onTargetCountChange,
  deadlineEnabled,
  onDeadlineEnabledChange,
  deadline,
  onDeadlineChange,
  error,
  targetCountError,
  deadlineError,
}: TargetDeadlinePickerProps) {
  return (
    <div className="flex flex-col gap-2">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        <Card className="gap-2 p-4">
          <Label className="flex items-center gap-2">
            <Checkbox
              checked={targetCountEnabled}
              onCheckedChange={(checked) => onTargetCountEnabledChange(checked === true)}
            />
            Target image count
          </Label>
          <Input
            type="number"
            min={1}
            placeholder="e.g. 500"
            disabled={!targetCountEnabled}
            value={targetCount ?? ''}
            onChange={(e) => onTargetCountChange(e.target.value ? Number(e.target.value) : undefined)}
          />
          {targetCountError && <p className="text-xs text-destructive">{targetCountError}</p>}
        </Card>

        <Card className="gap-2 p-4">
          <Label className="flex items-center gap-2">
            <Checkbox
              checked={deadlineEnabled}
              onCheckedChange={(checked) => onDeadlineEnabledChange(checked === true)}
            />
            Stop by date
          </Label>
          <Input
            type="datetime-local"
            disabled={!deadlineEnabled}
            value={deadline ?? ''}
            onChange={(e) => onDeadlineChange(e.target.value || undefined)}
          />
          {deadlineError && <p className="text-xs text-destructive">{deadlineError}</p>}
        </Card>
      </div>

      {error && <p className="text-xs text-destructive">{error}</p>}
    </div>
  )
}
