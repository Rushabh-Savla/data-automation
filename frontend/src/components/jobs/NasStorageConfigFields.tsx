import { useMemo, useState } from 'react'
import { Controller, type Control, type UseFormRegister } from 'react-hook-form'
import {
  CheckCircle2Icon,
  CornerLeftUpIcon,
  FolderIcon,
  FolderOpenIcon,
  FolderSearchIcon,
  HardDriveIcon,
  Loader2Icon,
  RefreshCwIcon,
  SearchIcon,
  XCircleIcon,
} from 'lucide-react'

import { listNasFolders, verifyNasFolder } from '@/api/nas'
import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { useNasConfig } from '@/hooks/useNasConfig'
import { cn } from '@/lib/utils'

function previewSlug(name: string | undefined): string {
  const slug = (name ?? '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60)
    .replace(/^-+|-+$/g, '')
  return slug ? slug : 'enter-a-job-name'
}

interface NasStorageConfigFieldsProps {
  jobName?: string
  control?: Control<JobCreateFormValues>
  register?: UseFormRegister<JobCreateFormValues>
}

export function NasStorageConfigFields({
  jobName,
  control,
}: NasStorageConfigFieldsProps) {
  const { data: config } = useNasConfig()

  // Explorer dialog state
  const [explorerOpen, setExplorerOpen] = useState(false)
  const [currentBrowsePath, setCurrentBrowsePath] = useState<string>('')
  const [allFolderPaths, setAllFolderPaths] = useState<string[] | null>(null)
  const [isLoadingFolders, setIsLoadingFolders] = useState(false)
  const [browseError, setBrowseError] = useState<string | null>(null)
  const [searchFilter, setSearchFilter] = useState('')

  // Direct path verification state
  const [isVerifying, setIsVerifying] = useState(false)
  const [verificationResult, setVerificationResult] = useState<{
    tested: boolean
    exists: boolean
    message: string
  } | null>(null)

  const isConfigured = Boolean(config?.configured)
  const jobFolder = previewSlug(jobName)

  // Single-request loader: fetches all NAS folders once over one connection
  async function fetchAllFolders() {
    setIsLoadingFolders(true)
    setBrowseError(null)
    try {
      const data = await listNasFolders('')
      // Prefer the full tree of all folders if present, fallback to folders list
      const paths = data.tree && data.tree.length > 0 ? data.tree : data.folders
      setAllFolderPaths(paths)
    } catch (err: unknown) {
      setBrowseError(err instanceof Error ? err.message : 'Could not browse NAS folders')
    } finally {
      setIsLoadingFolders(false)
    }
  }

  async function openExplorer(initialPath: string = '') {
    setExplorerOpen(true)
    setBrowseError(null)
    setSearchFilter('')
    setCurrentBrowsePath(initialPath.trim().replace(/^\/+|\/+$/g, ''))

    // Send request ONLY if not already fetched
    if (allFolderPaths === null) {
      await fetchAllFolders()
    }
  }

  // Compute immediate subfolders for the current browse path completely offline (in-memory)
  const visibleSubfolders = useMemo(() => {
    if (!allFolderPaths) return []
    const prefix = currentBrowsePath ? `${currentBrowsePath}/` : ''
    const childNames = new Set<string>()

    for (const p of allFolderPaths) {
      if (prefix ? p.startsWith(prefix) : true) {
        const remainder = prefix ? p.slice(prefix.length) : p
        const [segment] = remainder.split('/')
        if (segment) {
          if (!searchFilter.trim() || segment.toLowerCase().includes(searchFilter.trim().toLowerCase())) {
            childNames.add(segment)
          }
        }
      }
    }
    return Array.from(childNames).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }))
  }, [allFolderPaths, currentBrowsePath, searchFilter])

  // Compute parent path completely offline
  const parentBrowsePath = useMemo(() => {
    if (!currentBrowsePath) return null
    return currentBrowsePath.includes('/')
      ? currentBrowsePath.slice(0, currentBrowsePath.lastIndexOf('/'))
      : ''
  }, [currentBrowsePath])

  async function handleVerifyPath(path: string) {
    const clean = path.trim().replace(/^\/+|\/+$/g, '')
    if (!clean) {
      setVerificationResult({
        tested: true,
        exists: true,
        message: 'Root folder selected on NAS',
      })
      return
    }

    // Verify completely in-memory if tree is already loaded
    if (allFolderPaths && allFolderPaths.includes(clean)) {
      setVerificationResult({
        tested: true,
        exists: true,
        message: 'Folder exists on NAS (verified offline)',
      })
      return
    }

    setIsVerifying(true)
    try {
      const res = await verifyNasFolder(clean)
      setVerificationResult({
        tested: true,
        exists: res.exists,
        message: res.message,
      })
    } catch {
      setVerificationResult({
        tested: true,
        exists: false,
        message: 'Could not connect to verify folder path',
      })
    } finally {
      setIsVerifying(false)
    }
  }

  return (
    <Card className="gap-3 p-4">
      <CardHeader className="flex-row items-center justify-between p-0">
        <div className="flex items-center gap-2">
          <HardDriveIcon className="size-4 text-primary" />
          <CardTitle className="text-sm">NAS Storage Destination</CardTitle>
          <Badge variant="secondary" className="text-[11px] font-normal text-muted-foreground">
            Optional
          </Badge>
        </div>
        {isConfigured && config ? (
          <Badge variant="outline" className="text-xs uppercase font-mono">
            {config.protocol}: {config.host}
          </Badge>
        ) : (
          <Badge variant="secondary" className="text-xs">
            NAS Not Configured
          </Badge>
        )}
      </CardHeader>

      <CardContent className="flex flex-col gap-3 p-0">
        <p className="text-xs text-muted-foreground">
          <strong>Optional:</strong> Select or enter a NAS folder to automatically save this job's collected dataset to the NAS.
          If left empty, files will <strong>not</strong> be stored on the NAS and will only be stored locally, which you can export later.
        </p>

        {control ? (
          <Controller
            control={control}
            name="nasStoragePath"
            render={({ field }) => {
              const currentPath = field.value ?? ''

              return (
                <div className="flex flex-col gap-3">
                  <div className="flex flex-col sm:flex-row gap-2">
                    <div className="relative flex-1">
                      <Input
                        id="nas-storage-path-input"
                        placeholder="Leave empty to store locally only, or enter NAS path (e.g. ai_data_collection/test)"
                        value={currentPath}
                        onChange={(e) => {
                          field.onChange(e.target.value)
                          setVerificationResult(null)
                        }}
                        onBlur={() => {
                          if (currentPath.trim()) {
                            handleVerifyPath(currentPath)
                          }
                        }}
                      />
                    </div>

                    <div className="flex items-center gap-2">
                      {currentPath ? (
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          onClick={() => {
                            field.onChange('')
                            setVerificationResult(null)
                          }}
                          className="text-xs text-muted-foreground hover:text-foreground"
                          title="Remove NAS path (store in local storage only)"
                        >
                          Clear
                        </Button>
                      ) : null}

                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        disabled={isVerifying || !isConfigured || !currentPath.trim()}
                        onClick={() => handleVerifyPath(currentPath)}
                        className="gap-1.5"
                      >
                        {isVerifying ? (
                          <Loader2Icon className="size-3.5 animate-spin" />
                        ) : verificationResult?.exists ? (
                          <CheckCircle2Icon className="size-3.5 text-emerald-600" />
                        ) : verificationResult && !verificationResult.exists ? (
                          <XCircleIcon className="size-3.5 text-destructive" />
                        ) : null}
                        Verify Path
                      </Button>

                      <Button
                        type="button"
                        variant="secondary"
                        size="sm"
                        disabled={!isConfigured}
                        onClick={() => openExplorer(currentPath)}
                        className="gap-1.5 shrink-0"
                      >
                        <FolderSearchIcon className="size-3.5" />
                        Explore NAS Folders
                      </Button>
                    </div>
                  </div>

                  {/* Verification result pill */}
                  {verificationResult && (
                    <div className="flex items-center gap-1.5 text-xs">
                      {verificationResult.exists ? (
                        <span className="flex items-center gap-1 text-emerald-600 dark:text-emerald-400 font-medium">
                          <CheckCircle2Icon className="size-3.5" />
                          {verificationResult.message || 'Folder verified on NAS'}
                        </span>
                      ) : (
                        <span className="flex items-center gap-1 text-destructive font-medium">
                          <XCircleIcon className="size-3.5" />
                          {verificationResult.message || 'Folder not found on NAS'}
                        </span>
                      )}
                    </div>
                  )}

                  {/* Destination Status & Path Preview */}
                  {currentPath.trim() ? (
                    <div className="rounded-lg border border-primary/20 bg-primary/5 p-3 space-y-1">
                      <div className="flex items-center justify-between">
                        <span className="text-xs font-semibold text-primary">Target Storage Path on NAS:</span>
                        <Badge variant="outline" className="text-[10px] bg-background">Save to NAS</Badge>
                      </div>
                      <div className="font-mono text-xs text-primary bg-background rounded px-2.5 py-1.5 border border-border/60 break-all">
                        {currentPath.trim().replace(/\/+$/, '')}/{jobFolder}/...
                      </div>
                      <p className="text-[11px] text-muted-foreground">
                        Collected images will be saved inside the folder <strong>{jobFolder}</strong> under the selected location.
                      </p>
                    </div>
                  ) : (
                    <div className="rounded-lg border border-dashed border-border/70 bg-muted/20 p-3 space-y-1">
                      <div className="flex items-center gap-1.5 text-xs font-medium text-foreground">
                        <FolderIcon className="size-3.5 text-muted-foreground" />
                        <span>Local Storage Only (No NAS destination selected)</span>
                      </div>
                      <p className="text-[11px] text-muted-foreground">
                        Collected data will <strong>not</strong> be stored on the NAS. Files will remain on local storage and can be exported locally anytime via the job details page.
                      </p>
                    </div>
                  )}

                  {/* Folder Explorer Dialog */}
                  <Dialog open={explorerOpen} onOpenChange={setExplorerOpen}>
                    <DialogContent className="max-w-xl max-h-[85vh] flex flex-col gap-3">
                      <DialogHeader className="space-y-1">
                        <div className="flex items-center justify-between">
                          <DialogTitle className="flex items-center gap-2 text-base">
                            <FolderOpenIcon className="size-5 text-primary" />
                            NAS Folder Explorer
                          </DialogTitle>
                          <div className="flex items-center gap-2">
                            <Badge variant="outline" className="text-[10px] text-muted-foreground gap-1 font-normal">
                              <CheckCircle2Icon className="size-3 text-emerald-600" />
                              Single Request • Offline Browsing
                            </Badge>
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              className="h-7 w-7 p-0 text-muted-foreground hover:text-foreground"
                              onClick={fetchAllFolders}
                              disabled={isLoadingFolders}
                              title="Refresh folders from NAS (1 request)"
                            >
                              <RefreshCwIcon className={cn('size-3.5', isLoadingFolders && 'animate-spin')} />
                            </Button>
                          </div>
                        </div>
                        <DialogDescription className="text-xs">
                          Browse and select the target folder. All subfolders are loaded once in a single request and browsed offline without repeated calls to the NAS.
                        </DialogDescription>
                      </DialogHeader>

                      {/* Filter subfolders in current view */}
                      <div className="relative">
                        <SearchIcon className="absolute left-2.5 top-2.5 size-3.5 text-muted-foreground" />
                        <Input
                          placeholder="Filter subfolders in current directory..."
                          value={searchFilter}
                          onChange={(e) => setSearchFilter(e.target.value)}
                          className="h-8 pl-8 text-xs"
                        />
                      </div>

                      {/* Breadcrumbs Navigation */}
                      <div className="flex items-center gap-1 overflow-x-auto rounded-md bg-muted/40 p-2 text-xs font-mono">
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className={cn('h-6 px-1.5 text-xs', !currentBrowsePath && 'font-bold text-foreground bg-accent')}
                          onClick={() => {
                            setCurrentBrowsePath('')
                            setSearchFilter('')
                          }}
                        >
                          Root
                        </Button>
                        {currentBrowsePath.split('/').filter(Boolean).map((segment, idx, arr) => {
                          const subpath = arr.slice(0, idx + 1).join('/')
                          const isLast = idx === arr.length - 1
                          return (
                            <span key={subpath} className="flex items-center gap-1">
                              <span className="text-muted-foreground">/</span>
                              <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                className={cn(
                                  'h-6 px-1.5 text-xs',
                                  isLast && 'font-bold text-foreground bg-accent',
                                )}
                                onClick={() => {
                                  setCurrentBrowsePath(subpath)
                                  setSearchFilter('')
                                }}
                              >
                                {segment}
                              </Button>
                            </span>
                          )
                        })}
                      </div>

                      {/* Parent level up button */}
                      {parentBrowsePath !== null && (
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="w-fit gap-1.5 text-xs h-7"
                          disabled={isLoadingFolders}
                          onClick={() => {
                            setCurrentBrowsePath(parentBrowsePath)
                            setSearchFilter('')
                          }}
                        >
                          <CornerLeftUpIcon className="size-3.5" />
                          Up to parent folder
                        </Button>
                      )}

                      {/* Folder list */}
                      <div className="flex-1 min-h-[220px] max-h-[300px] overflow-y-auto rounded-md border p-1">
                        {isLoadingFolders ? (
                          <div className="flex flex-col items-center justify-center h-48 gap-2 text-muted-foreground text-xs">
                            <Loader2Icon className="size-6 animate-spin text-primary" />
                            Loading folders from NAS (single request)…
                          </div>
                        ) : browseError ? (
                          <div className="flex flex-col items-center justify-center h-48 p-4 text-center text-xs text-destructive">
                            <XCircleIcon className="size-6 mb-1" />
                            <p>{browseError}</p>
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              onClick={fetchAllFolders}
                              className="mt-2 text-xs"
                            >
                              Try Again
                            </Button>
                          </div>
                        ) : visibleSubfolders.length === 0 ? (
                          <div className="flex flex-col items-center justify-center h-48 text-muted-foreground text-xs">
                            <FolderOpenIcon className="size-8 stroke-1 text-muted-foreground/60 mb-1" />
                            {searchFilter ? `No folders matching "${searchFilter}"` : 'No subfolders found in this directory.'}
                          </div>
                        ) : (
                          <div className="grid grid-cols-1 sm:grid-cols-2 gap-1">
                            {visibleSubfolders.map((folder) => {
                              const targetSubpath = currentBrowsePath
                                ? `${currentBrowsePath}/${folder}`
                                : folder

                              return (
                                <button
                                  key={folder}
                                  type="button"
                                  onClick={() => {
                                    setCurrentBrowsePath(targetSubpath)
                                    setSearchFilter('')
                                  }}
                                  className="flex items-center justify-between rounded-md px-3 py-2 text-left text-xs hover:bg-accent transition-colors border border-transparent hover:border-border cursor-pointer group"
                                >
                                  <div className="flex items-center gap-2 truncate">
                                    <FolderIcon className="size-4 text-primary shrink-0 group-hover:scale-105 transition-transform" />
                                    <span className="truncate font-medium">{folder}</span>
                                  </div>
                                  <span className="text-[10px] text-muted-foreground ml-2 group-hover:text-foreground">Open →</span>
                                </button>
                              )
                            })}
                          </div>
                        )}
                      </div>

                      <DialogFooter className="flex flex-col sm:flex-row items-center justify-between gap-2 pt-2 border-t">
                        <div className="text-xs text-muted-foreground truncate max-w-xs">
                          Selected: <span className="font-mono text-foreground font-medium">{currentBrowsePath || '(Root)'}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            onClick={() => {
                              field.onChange('')
                              setVerificationResult(null)
                              setExplorerOpen(false)
                            }}
                            className="text-xs text-muted-foreground"
                          >
                            Store Locally Only
                          </Button>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={() => setExplorerOpen(false)}
                          >
                            Cancel
                          </Button>
                          <Button
                            type="button"
                            size="sm"
                            onClick={() => {
                              field.onChange(currentBrowsePath)
                              setVerificationResult({
                                tested: true,
                                exists: true,
                                message: 'Folder selected and verified on NAS',
                              })
                              setExplorerOpen(false)
                            }}
                          >
                            Select This Folder
                          </Button>
                        </div>
                      </DialogFooter>
                    </DialogContent>
                  </Dialog>
                </div>
              )
            }}
          />
        ) : null}
      </CardContent>
    </Card>
  )
}
