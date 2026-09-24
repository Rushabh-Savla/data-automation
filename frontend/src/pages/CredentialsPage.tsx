import { Topbar } from '@/components/layout/Topbar'
import { Card, CardContent } from '@/components/ui/card'
import { CredentialList } from '@/components/credentials/CredentialList'
import { CredentialFormDialog } from '@/components/credentials/CredentialFormDialog'
import { NasConnectionCard } from '@/components/credentials/NasConnectionCard'
import { useCredentials } from '@/hooks/useCredentials'

export function CredentialsPage() {
  const { data: credentials, isLoading } = useCredentials()

  return (
    <>
      <Topbar title="Credentials" right={<CredentialFormDialog />} />

      <div className="flex flex-col gap-4 p-6">
        <NasConnectionCard />

        <Card>
          <CardContent className="px-0">
            <CredentialList credentials={credentials} isLoading={isLoading} />
          </CardContent>
        </Card>
      </div>
    </>
  )
}
