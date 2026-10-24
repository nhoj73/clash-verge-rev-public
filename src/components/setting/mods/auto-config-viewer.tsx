import {
  Button,
  Chip,
  InputAdornment,
  List,
  ListItem,
  ListItemText,
  Stack,
  TextField,
  Typography,
} from '@mui/material'
import { useLockFn } from 'ahooks'
import { forwardRef, useImperativeHandle, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { BaseDialog, DialogRef, Switch, TooltipIcon } from '@/components/base'
import {
  autoConfigStatusKey,
  useAutoConfigStatus,
} from '@/hooks/use-auto-config'
import { useVerge } from '@/hooks/use-verge'
import {
  runAutoConfigNow,
  type AgentFailure,
  type AgentPhase,
  type ProbeVerdict,
} from '@/services/cmds'
import { mutate } from '@/services/mutate'
import { showNotice } from '@/services/notice-service'
import type { TranslationKey } from '@/types/generated/i18n-keys'

const MIN_INTERVAL_MINUTES = 5
const MAX_INTERVAL_MINUTES = 1440
const MIN_FAILURE_THRESHOLD = 1
const MAX_FAILURE_THRESHOLD = 10

const PHASE_KEYS: Readonly<Record<AgentPhase, TranslationKey>> = {
  disabled: 'settings.modals.autoConfigAgent.status.phase.disabled',
  idle: 'settings.modals.autoConfigAgent.status.phase.idle',
  probing: 'settings.modals.autoConfigAgent.status.phase.probing',
  searching: 'settings.modals.autoConfigAgent.status.phase.searching',
  validating: 'settings.modals.autoConfigAgent.status.phase.validating',
  switching: 'settings.modals.autoConfigAgent.status.phase.switching',
  benchmarking: 'settings.modals.autoConfigAgent.status.phase.benchmarking',
  verifying: 'settings.modals.autoConfigAgent.status.phase.verifying',
  adopted: 'settings.modals.autoConfigAgent.status.phase.adopted',
  reverted: 'settings.modals.autoConfigAgent.status.phase.reverted',
  abandoned: 'settings.modals.autoConfigAgent.status.phase.abandoned',
  failed: 'settings.modals.autoConfigAgent.status.phase.failed',
}

type PhaseColor = 'default' | 'primary' | 'success' | 'warning' | 'error'

// Colors carry the verdict so a glance works without reading the words: blue while it is working,
// green on success, amber for a step aside, red only when the user has to intervene.
const PHASE_COLORS: Readonly<Record<AgentPhase, PhaseColor>> = {
  disabled: 'default',
  idle: 'default',
  probing: 'primary',
  searching: 'primary',
  validating: 'primary',
  switching: 'primary',
  benchmarking: 'primary',
  verifying: 'primary',
  adopted: 'success',
  reverted: 'warning',
  abandoned: 'warning',
  failed: 'error',
}

const VERDICT_KEYS: Readonly<Record<ProbeVerdict, TranslationKey>> = {
  reachable: 'settings.modals.autoConfigAgent.status.verdict.reachable',
  localNetworkDown:
    'settings.modals.autoConfigAgent.status.verdict.localNetworkDown',
  proxyFailure: 'settings.modals.autoConfigAgent.status.verdict.proxyFailure',
}

// The backend sends one of these codes, never a diagnostic string: what to do next is in the
// wording, and the English detail stays in the log.
const FAILURE_KEYS: Readonly<Record<AgentFailure, TranslationKey>> = {
  tokenInvalid: 'settings.modals.autoConfigAgent.status.failure.tokenInvalid',
  rateLimited: 'settings.modals.autoConfigAgent.status.failure.rateLimited',
  networkBlocked:
    'settings.modals.autoConfigAgent.status.failure.networkBlocked',
  noUsableConfig:
    'settings.modals.autoConfigAgent.status.failure.noUsableConfig',
  switchBlocked: 'settings.modals.autoConfigAgent.status.failure.switchBlocked',
  switchFailed: 'settings.modals.autoConfigAgent.status.failure.switchFailed',
  stillUnreachable:
    'settings.modals.autoConfigAgent.status.failure.stillUnreachable',
  localNetworkDown:
    'settings.modals.autoConfigAgent.status.failure.localNetworkDown',
  rollbackFailed:
    'settings.modals.autoConfigAgent.status.failure.rollbackFailed',
}

const DEFAULT_INTERVAL_MINUTES = 30
const DEFAULT_FAILURE_THRESHOLD = 3

const clampValue = (value: number, min: number, max: number) =>
  Math.min(max, Math.max(min, value))

const readNumber = (raw: string, fallback: number) => {
  const parsed = Number.parseInt(raw, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

export const AutoConfigViewer = forwardRef<DialogRef>((props, ref) => {
  const { t } = useTranslation()
  const { verge, patchVerge } = useVerge()
  const [open, setOpen] = useState(false)
  const { data: status } = useAutoConfigStatus(open)

  const savedValues = {
    githubToken: verge?.github_token?.trim() ?? '',
    enableAgent: verge?.enable_auto_config_agent ?? false,
    intervalMinutes:
      verge?.auto_config_agent_interval_minutes ?? DEFAULT_INTERVAL_MINUTES,
    failureThreshold:
      verge?.auto_config_agent_failure_threshold ?? DEFAULT_FAILURE_THRESHOLD,
  }

  const [values, setValues] = useState(savedValues)

  // Closing this dialog throws the edits away, so say so while they are still on the screen.
  const unsaved =
    values.githubToken.trim() !== savedValues.githubToken ||
    values.enableAgent !== savedValues.enableAgent ||
    values.intervalMinutes !== savedValues.intervalMinutes ||
    values.failureThreshold !== savedValues.failureThreshold

  useImperativeHandle(ref, () => ({
    open: () => {
      setOpen(true)
      setValues(savedValues)
    },
    close: () => setOpen(false),
  }))

  const onSave = useLockFn(async () => {
    try {
      await patchVerge({
        // An empty string means "no token". Sending null would not clear it: `patch_config` skips
        // every field the payload leaves as None.
        github_token: values.githubToken.trim(),
        enable_auto_config_agent: values.enableAgent,
        auto_config_agent_interval_minutes: clampValue(
          values.intervalMinutes,
          MIN_INTERVAL_MINUTES,
          MAX_INTERVAL_MINUTES,
        ),
        auto_config_agent_failure_threshold: clampValue(
          values.failureThreshold,
          MIN_FAILURE_THRESHOLD,
          MAX_FAILURE_THRESHOLD,
        ),
      })
      setOpen(false)
    } catch (err) {
      showNotice.error(err)
    }
  })

  const onRunNow = useLockFn(async () => {
    try {
      await mutate(() => runAutoConfigNow(), {
        id: 'run-auto-config-now',
        revalidate: [autoConfigStatusKey],
        successNotice:
          'settings.modals.autoConfigAgent.notifications.roundStarted',
      })
    } catch {
      // A rejected round shows its own notice; a busy agent is not a failure to repeat.
    }
  })

  return (
    <BaseDialog
      open={open}
      title={t('settings.modals.autoConfigAgent.title')}
      contentSx={{ width: 450 }}
      okBtn={t('shared.actions.save')}
      cancelBtn={t('shared.actions.cancel')}
      onClose={() => setOpen(false)}
      onCancel={() => setOpen(false)}
      onOk={onSave}
    >
      <List>
        <ListItem sx={{ padding: '5px 2px' }}>
          <Typography variant="body2" sx={{ mb: 1 }}>
            {t('settings.modals.autoConfigAgent.helper')}
          </Typography>
        </ListItem>

        <ListItem sx={{ padding: '5px 2px' }}>
          <ListItemText
            primary={t('settings.modals.autoConfigAgent.fields.enable')}
          />
          <Switch
            edge="end"
            checked={values.enableAgent}
            onChange={(_, c) => setValues((v) => ({ ...v, enableAgent: c }))}
          />
        </ListItem>

        <ListItem sx={{ padding: '5px 2px' }}>
          <ListItemText
            primary={t('settings.modals.autoConfigAgent.fields.token')}
            sx={{ maxWidth: 'fit-content' }}
          />
          <TooltipIcon
            title={t('settings.modals.autoConfigAgent.tooltips.token')}
            sx={{ opacity: '0.7' }}
          />
          <TextField
            autoComplete="off"
            size="small"
            autoCorrect="off"
            autoCapitalize="off"
            spellCheck="false"
            sx={{ width: 250, marginLeft: 'auto' }}
            placeholder={t(
              'settings.modals.autoConfigAgent.placeholders.token',
            )}
            value={values.githubToken}
            onChange={(e) =>
              setValues((v) => ({ ...v, githubToken: e.target.value }))
            }
          />
        </ListItem>

        <ListItem sx={{ padding: '5px 2px' }}>
          <ListItemText
            primary={t('settings.modals.autoConfigAgent.fields.interval')}
            sx={{ maxWidth: 'fit-content' }}
          />
          <TextField
            autoComplete="off"
            size="small"
            type="number"
            autoCorrect="off"
            autoCapitalize="off"
            spellCheck="false"
            sx={{ width: 160, marginLeft: 'auto' }}
            value={values.intervalMinutes}
            onChange={(e) =>
              setValues((v) => ({
                ...v,
                intervalMinutes: readNumber(
                  e.target.value,
                  MIN_INTERVAL_MINUTES,
                ),
              }))
            }
            slotProps={{
              input: {
                endAdornment: (
                  <InputAdornment position="end">
                    {t('shared.units.minutes')}
                  </InputAdornment>
                ),
              },
              htmlInput: {
                min: MIN_INTERVAL_MINUTES,
                max: MAX_INTERVAL_MINUTES,
                inputMode: 'numeric',
              },
            }}
          />
        </ListItem>

        <ListItem sx={{ padding: '5px 2px' }}>
          <ListItemText
            primary={t('settings.modals.autoConfigAgent.fields.threshold')}
            sx={{ maxWidth: 'fit-content' }}
          />
          <TooltipIcon
            title={t('settings.modals.autoConfigAgent.tooltips.threshold')}
            sx={{ opacity: '0.7' }}
          />
          <TextField
            autoComplete="off"
            size="small"
            type="number"
            autoCorrect="off"
            autoCapitalize="off"
            spellCheck="false"
            sx={{ width: 160, marginLeft: 'auto' }}
            value={values.failureThreshold}
            onChange={(e) =>
              setValues((v) => ({
                ...v,
                failureThreshold: readNumber(
                  e.target.value,
                  MIN_FAILURE_THRESHOLD,
                ),
              }))
            }
            slotProps={{
              htmlInput: {
                min: MIN_FAILURE_THRESHOLD,
                max: MAX_FAILURE_THRESHOLD,
                inputMode: 'numeric',
              },
            }}
          />
        </ListItem>

        <ListItem
          divider
          sx={{
            padding: '12px 2px 6px',
            flexDirection: 'column',
            alignItems: 'flex-start',
          }}
        >
          <Stack
            direction="row"
            spacing={1}
            sx={{ alignItems: 'center', width: '100%' }}
          >
            <Typography sx={{ fontWeight: 'bold' }}>
              {t('settings.modals.autoConfigAgent.status.title')}
            </Typography>
            <Button
              size="small"
              variant="outlined"
              sx={{ marginLeft: 'auto' }}
              disabled={status?.roundRunning === true}
              onClick={() => void onRunNow()}
            >
              {t('settings.modals.autoConfigAgent.actions.runNow')}
            </Button>
          </Stack>

          <Stack spacing={0.5} sx={{ marginTop: 1, width: '100%' }}>
            {!status && (
              <Typography variant="body2" sx={{ color: 'text.secondary' }}>
                {t('settings.modals.autoConfigAgent.status.waiting')}
              </Typography>
            )}
            {status && (
              <Chip
                size="small"
                color={PHASE_COLORS[status.phase]}
                label={t(PHASE_KEYS[status.phase])}
                sx={{ alignSelf: 'flex-start' }}
              />
            )}
            {status?.lastVerdict && (
              <Typography variant="body2">
                {t(VERDICT_KEYS[status.lastVerdict])}
              </Typography>
            )}
            {status?.failure && (
              <Typography
                variant="body2"
                sx={{
                  color:
                    status.failure === 'rollbackFailed'
                      ? 'error.main'
                      : 'text.secondary',
                }}
              >
                {t(FAILURE_KEYS[status.failure])}
              </Typography>
            )}
            {status?.source && (
              <Typography
                variant="body2"
                sx={{ color: 'text.secondary', wordBreak: 'break-all' }}
              >
                {t('settings.modals.autoConfigAgent.status.source', {
                  source: status.source,
                })}
              </Typography>
            )}
            {status && !status.enabled && (
              <Typography variant="body2" sx={{ color: 'text.secondary' }}>
                {t('settings.modals.autoConfigAgent.hints.disabled')}
              </Typography>
            )}
            {unsaved && (
              <Typography variant="body2" sx={{ color: 'warning.main' }}>
                {t('settings.modals.autoConfigAgent.hints.unsaved')}
              </Typography>
            )}
          </Stack>
        </ListItem>
      </List>
    </BaseDialog>
  )
})
