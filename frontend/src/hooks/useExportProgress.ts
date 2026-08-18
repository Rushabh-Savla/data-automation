import { useQuery } from '@tanstack/react-query'

import { getExport } from '@/api/exports'
import { exportKeys } from '@/hooks/queryKeys'

/**
 * REST poll fallback for export status — the WebSocket's export_progress/
 * export_complete/export_failed events (see useJobProgressSocket) patch
 * this same cache entry directly when the socket is live, same pattern as
 * job progress: poll while not otherwise kept fresh, stop once terminal.
 */
export function useExportProgress(exportId: string | undefined) {
  return useQuery({
    queryKey: exportKeys.detail(exportId ?? ''),
    queryFn: () => getExport(exportId!),
    enabled: !!exportId,
    refetchInterval: (query) => {
      const status = query.state.data?.status
      if (status === 'completed' || status === 'failed') return false
      return 2_000
    },
  })
}
