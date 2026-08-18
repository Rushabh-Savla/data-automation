import { chromium } from 'playwright'

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1400, height: 1000 } })
page.on('pageerror', (e) => console.log('[pageerror]', e.message))
page.on('console', (m) => { if (m.type() === 'error') console.log('[console.error]', m.text()) })

// Known job with valid (post bind-mount-fix) nas_path assets
await page.goto('http://localhost:5173/jobs/1345fe77-f931-4b4d-8917-6479e452742e/assets', { waitUntil: 'networkidle' })
await page.waitForTimeout(800)

const assetCount = await page.locator('img[alt]').count()
console.log('Assets: image count on page =', assetCount)

const firstCard = page.locator('.group').first()
await firstCard.waitFor({ state: 'visible', timeout: 10000 })
await firstCard.hover()
await page.waitForTimeout(300)
const downloadBtn = firstCard.locator('button[title="Download"]')
console.log('Assets: download button visible on hover =', (await downloadBtn.count()) > 0)

// Try search filter
await page.getByPlaceholder('Search by filename or dataset…').fill('0000000')
await page.waitForTimeout(600)
await page.waitForLoadState('networkidle')
const filteredCount = await page.locator('img[alt]').count()
console.log('Assets: image count after search "0000000" =', filteredCount)

await page.screenshot({ path: `${process.cwd()}/verify-assets2.png`, fullPage: true })
await browser.close()
