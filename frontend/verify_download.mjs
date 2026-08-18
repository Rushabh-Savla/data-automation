import { chromium } from 'playwright'
import fs from 'fs'

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1400, height: 1000 }, acceptDownloads: true })
page.on('pageerror', (e) => console.log('[pageerror]', e.message))

await page.goto('http://localhost:5173/jobs/1345fe77-f931-4b4d-8917-6479e452742e/assets', { waitUntil: 'networkidle' })
await page.waitForTimeout(2000)

const firstCard = page.locator('.group').first()
await firstCard.hover()
await page.waitForTimeout(300)

const [download] = await Promise.all([
  page.waitForEvent('download', { timeout: 10000 }),
  firstCard.locator('button[title="Download"]').click(),
])
const savePath = `${process.cwd()}/downloaded_${download.suggestedFilename()}`
await download.saveAs(savePath)
const stats = fs.statSync(savePath)
console.log('Downloaded file:', download.suggestedFilename(), 'size:', stats.size, 'bytes')
fs.unlinkSync(savePath)

await browser.close()
