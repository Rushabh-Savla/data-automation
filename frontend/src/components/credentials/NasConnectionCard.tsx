import { HardDriveIcon } from 'lucide-react'
import { Link } from 'react-router-dom'

import type { NasConfig, NasProtocol } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useNasConfig } from '@/hooks/useNasConfig'

const PROTOCOL_NAMES: Record<NasProtocol, string> = {
  smb: 'SMB / CIFS',
  sftp: 'SFTP',
  ftp: 'FTP / FTPS',
  webdav: 'WebDAV',
  nfs: 'NFS',
  local: 'Mounted path',
}

function describe(config: NasConfig): string {
  const where = config.protocol === 'local' ? config.share : [config.host, config.share].filter(Boolean).join(' / ')
  const access = config.auth_mode === 'password' ? `signed in as ${config.username}` : 'open, no password'
  const storage = config.use_as_storage ? 'filtered images are stored here' : 'not used to store images'
  return `${PROTOCOL_NAMES[config.protocol]} · ${where} · ${access} · ${storage}`
}

/** NAS lives on its own page (host, share, sign-in and folders do not fit the
 * one-key-per-row credential list); this card puts it where people look for
 * credentials and links to it. */
export function NasConnectionCard() {
  const { data: config, isLoading } = useNasConfig()

  return (
    <Card>
      <CardContent className="flex items-center gap-4">
        <HardDriveIcon className="size-5 shrink-0 text-muted-foreground" />
        <div className="flex min-w-0 flex-1 flex-col gap-1">
          <div className="flex items-center gap-2">
            <span className="font-medium">NAS</span>
            {isLoading ? (
              <Skeleton className="h-5 w-24" />
            ) : (
              <Badge variant={config?.configured ? 'success' : 'secondary'}>
                {config?.configured ? 'Connected' : 'Not configured'}
              </Badge>
            )}
          </div>
          <p className="truncate text-sm text-muted-foreground">
            {config?.configured
              ? describe(config)
              : 'Connect a network drive to collect images from it and keep the images that pass filtering.'}
          </p>
        </div>
        <Button asChild variant="outline" size="sm">
          <Link to="/nas">{config?.configured ? 'Edit connection' : 'Connect NAS'}</Link>
        </Button>
      </CardContent>
    </Card>
  )
}
