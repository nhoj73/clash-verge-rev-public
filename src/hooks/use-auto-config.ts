import { getAutoConfigStatus } from '@/services/cmds'
import { useQuery } from '@/services/query-client'

export const autoConfigStatusKey = ['autoConfigStatus'] as const

/**
 * The agent phase changes on its own schedule, so the panel re-reads it while it is open and stops
 * polling as soon as it closes.
 */
export function useAutoConfigStatus(enabled: boolean) {
  return useQuery({
    queryKey: autoConfigStatusKey,
    queryFn: getAutoConfigStatus,
    enabled,
    staleTime: 1000,
    refetchInterval: enabled ? 2000 : false,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    retry: 1,
  })
}
