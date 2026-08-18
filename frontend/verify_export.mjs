import { chromium } from 'playwright'
import fs from 'fs'

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1400, height: 1000 }, acceptDownloads: true })
page.on('pageerror', (e) => console.log('[pageerror]', e.message))
page.on('console', (m) => { if (m.type() === 'error') console.log('[console.error]', m.text()) })

await page.goto('http://localhost:5173/jobs/1345fe77-f931-4b4d-8917-6479e452742e/assets', { waitUntil: 'networkidle' })
await page.waitForTimeout(1000)

await page.getByRole('button', { name: 'Export' }).click()
await page.waitForTimeout(400)
await page.screenshot({ path: `${process.cwd()}/verify-export-dialog.png` })

// Find and click the confirm/create button inside the dialog
const dialog = page.locator('[role="dialog"]')
const createBtn = dialog.getByRole('button', { name: /export/i }).last()
await createBtn.click()
console.log('Export triggered, waiting for completion...')

// Wait for the export status panel to show "completed" and a download button
await page.waitForSelector('text=/completed/i', { timeout: 60000 })
console.log('Export completed banner appeared')
await page.waitForTimeout(500)
await page.screenshot({ path: `${process.cwd()}/verify-export-complete.png`, fullPage: true })

const downloadBtn = page.getByRole('button', { name: /download/i }).first()
const [download] = await Promise.all([
  page.waitForEvent('download', { timeout: 15000 }),
  downloadBtn.click(),
])
const savePath = `${process.cwd()}/downloaded_${download.suggestedFilename()}`
await download.saveAs(savePath)
const stats = fs.statSync(savePath)
console.log('Downloaded export:', download.suggestedFilename(), 'size:', stats.size, 'bytes')
fs.unlinkSync(savePath)

await browser.close()
