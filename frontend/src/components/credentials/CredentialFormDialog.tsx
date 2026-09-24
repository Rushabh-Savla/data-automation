import { useState } from 'react'
import { PlusIcon } from 'lucide-react'

import type { CredentialResponse, SourceType } from '@/api/types'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { sourceTypeLabels } from '@/lib/enumLabels'
import { useCreateCredential } from '@/hooks/useCreateCredential'
import { useUpdateCredential } from '@/hooks/useUpdateCredential'

const SOURCE_KEY_HINTS: Record<SourceType, string[]> = {
  roboflow: ['ROBOFLOW_API_KEY'],
  kaggle: ['KAGGLE_USERNAME', 'KAGGLE_KEY'],
  youtube: ['YOUTUBE_API_KEY'],
  web: [],
  coco: [],
  open_images: [],
  pascal_voc: [],
  visual_genome: [],
  davis: [],
  sun_rgbd: [],
  nuscenes: [],
  kitti: [],
  nas_share: [],
}

interface CredentialFormDialogProps {
  credential?: CredentialResponse
  trigger?: React.ReactNode
}

export function CredentialFormDialog({ credential, trigger }: CredentialFormDialogProps) {
  const isEdit = !!credential
  const [open, setOpen] = useState(false)
  const [sourceType, setSourceType] = useState<SourceType>(credential?.source_type ?? 'roboflow')
  const [label, setLabel] = useState(credential?.label ?? '')
  const [keyName, setKeyName] = useState(credential?.key_name ?? SOURCE_KEY_HINTS.roboflow[0] ?? '')
  const [value, setValue] = useState('')

  const createCredential = useCreateCredential()
  const updateCredential = useUpdateCredential()
  const isPending = createCredential.isPending || updateCredential.isPending

  async function handleSubmit() {
    if (isEdit) {
      await updateCredential.mutateAsync({
        credentialId: credential.id,
        payload: { label, ...(value ? { value } : {}) },
      })
    } else {
      await createCredential.mutateAsync({ source_type: sourceType, label, key_name: keyName, value })
    }
    setOpen(false)
    setValue('')
  }

  const canSubmit = label.trim() !== '' && keyName.trim() !== '' && (isEdit ? true : value.trim() !== '')

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        {trigger ?? (
          <Button size="sm" className="gap-1.5">
            <PlusIcon className="size-4" /> Add credential
          </Button>
        )}
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{isEdit ? 'Edit credential' : 'Add credential'}</DialogTitle>
          <DialogDescription>
            {isEdit
              ? 'Values are encrypted at rest and never shown again after saving.'
              : 'Stored encrypted — used by the matching connector automatically, no need to edit .env.'}
          </DialogDescription>
        </DialogHeader>

        <div className="flex flex-col gap-4">
          {!isEdit && (
            <div className="flex flex-col gap-1.5">
              <Label>Source</Label>
              <Select
                value={sourceType}
                onValueChange={(v) => {
                  setSourceType(v as SourceType)
                  setKeyName(SOURCE_KEY_HINTS[v as SourceType][0] ?? '')
                }}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {/* NAS is configured on its own page, not with an API key. */}
                  {Object.entries(sourceTypeLabels)
                    .filter(([v]) => v !== 'nas_share')
                    .map(([v, label]) => (
                      <SelectItem key={v} value={v}>
                        {label}
                      </SelectItem>
                    ))}
                </SelectContent>
              </Select>
            </div>
          )}

          <div className="flex flex-col gap-1.5">
            <Label>Label</Label>
            <Input value={label} onChange={(e) => setLabel(e.target.value)} placeholder="e.g. Primary account" />
          </div>

          <div className="flex flex-col gap-1.5">
            <Label>Key name</Label>
            <Input
              value={keyName}
              onChange={(e) => setKeyName(e.target.value)}
              placeholder="e.g. ROBOFLOW_API_KEY"
              disabled={isEdit}
              list="key-name-hints"
            />
            <datalist id="key-name-hints">
              {SOURCE_KEY_HINTS[sourceType].map((hint) => (
                <option key={hint} value={hint} />
              ))}
            </datalist>
          </div>

          <div className="flex flex-col gap-1.5">
            <Label>Value {isEdit && '(leave blank to keep the current value)'}</Label>
            <Input
              type="password"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder={isEdit ? credential.masked_value : 'Paste the API key/secret'}
            />
          </div>
        </div>

        <DialogFooter>
          <Button onClick={handleSubmit} disabled={!canSubmit || isPending}>
            {isPending ? 'Saving…' : 'Save'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
