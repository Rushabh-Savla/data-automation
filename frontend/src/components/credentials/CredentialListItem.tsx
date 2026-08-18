import { PencilIcon } from 'lucide-react'

import type { CredentialResponse } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { TableCell, TableRow } from '@/components/ui/table'
import { sourceTypeLabels } from '@/lib/enumLabels'
import { formatRelative } from '@/lib/dateFormat'
import { CredentialFormDialog } from '@/components/credentials/CredentialFormDialog'
import { DeleteCredentialDialog } from '@/components/credentials/DeleteCredentialDialog'
import { useUpdateCredential } from '@/hooks/useUpdateCredential'

export function CredentialListItem({ credential }: { credential: CredentialResponse }) {
  const updateCredential = useUpdateCredential()

  return (
    <TableRow>
      <TableCell className="font-medium">{sourceTypeLabels[credential.source_type]}</TableCell>
      <TableCell>{credential.label}</TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">{credential.key_name}</TableCell>
      <TableCell className="font-mono text-xs">{credential.masked_value}</TableCell>
      <TableCell>
        <Badge
          variant={credential.is_active ? 'success' : 'outline'}
          className="cursor-pointer"
          onClick={() =>
            updateCredential.mutate({ credentialId: credential.id, payload: { is_active: !credential.is_active } })
          }
        >
          {credential.is_active ? 'Active' : 'Inactive'}
        </Badge>
      </TableCell>
      <TableCell className="text-muted-foreground">{formatRelative(credential.last_used_at)}</TableCell>
      <TableCell>
        <div className="flex items-center gap-1">
          <CredentialFormDialog
            credential={credential}
            trigger={
              <Button variant="ghost" size="icon">
                <PencilIcon className="size-4" />
              </Button>
            }
          />
          <DeleteCredentialDialog credentialId={credential.id} label={credential.label} />
        </div>
      </TableCell>
    </TableRow>
  )
}
