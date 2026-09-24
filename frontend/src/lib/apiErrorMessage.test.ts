import { describe, expect, it } from 'vitest'

import { ApiError } from '@/api/client'
import { apiErrorMessage } from '@/lib/apiErrorMessage'

describe('apiErrorMessage', () => {
  it('uses a plain string detail as-is', () => {
    expect(apiErrorMessage(new ApiError(503, 'The NAS is offline'), 'fallback')).toBe('The NAS is offline')
  })

  it('reads the msg of each FastAPI validation error and drops the pydantic prefix', () => {
    const detail = [
      { loc: ['body'], msg: 'Value error, Mounted folder path must be an absolute path', type: 'value_error' },
      { loc: ['body', 'port'], msg: 'Input should be less than or equal to 65535', type: 'less_than_equal' },
    ]
    expect(apiErrorMessage(new ApiError(422, detail), 'fallback')).toBe(
      'Mounted folder path must be an absolute path. Input should be less than or equal to 65535',
    )
  })

  it('never renders [object Object]', () => {
    expect(apiErrorMessage(new ApiError(422, [{ loc: [], type: 'x' }]), 'fallback')).not.toContain('[object')
  })

  it('falls back for a non-ApiError', () => {
    expect(apiErrorMessage(new TypeError('network down'), 'Failed to save NAS connection')).toBe(
      'Failed to save NAS connection',
    )
  })
})
