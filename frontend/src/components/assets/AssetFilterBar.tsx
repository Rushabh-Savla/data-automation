import type { AssetStatus, SourceType } from '@/api/types'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { assetStatusLabels, sourceTypeLabels } from '@/lib/enumLabels'

const ALL = '__all__'

interface AssetFilterBarProps {
  status: AssetStatus | undefined
  onStatusChange: (value: AssetStatus | undefined) => void
  sourceType: SourceType | undefined
  onSourceTypeChange: (value: SourceType | undefined) => void
}

export function AssetFilterBar({ status, onStatusChange, sourceType, onSourceTypeChange }: AssetFilterBarProps) {
  return (
    <div className="flex gap-3">
      <Select
        value={status ?? ALL}
        onValueChange={(v) => onStatusChange(v === ALL ? undefined : (v as AssetStatus))}
      >
        <SelectTrigger className="w-48">
          <SelectValue placeholder="All statuses" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL}>All statuses</SelectItem>
          {Object.entries(assetStatusLabels).map(([value, label]) => (
            <SelectItem key={value} value={value}>
              {label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select
        value={sourceType ?? ALL}
        onValueChange={(v) => onSourceTypeChange(v === ALL ? undefined : (v as SourceType))}
      >
        <SelectTrigger className="w-48">
          <SelectValue placeholder="All sources" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL}>All sources</SelectItem>
          {Object.entries(sourceTypeLabels).map(([value, label]) => (
            <SelectItem key={value} value={value}>
              {label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  )
}
