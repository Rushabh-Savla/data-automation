import { chromium } from 'playwright'

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1400, height: 1000 } })
page.on('pageerror', (e) => console.log('[pageerror]', e.message))
page.on('console', (m) => { if (m.type() === 'error') console.log('[console.error]', m.text()) })

// --- Jobs page: search + pagination ---
await page.goto('http://localhost:5173/jobs', { waitUntil: 'networkidle' })
await page.waitForTimeout(500)

let rowCount = await page.locator('table tbody tr').count()
console.log('Jobs: initial row count =', rowCount)

const paginationText = await page.locator('text=/Page \\d+ of \\d+/').first().textContent().catch(() => null)
console.log('Jobs: pagination label =', paginationText)

// Search
await page.getByPlaceholder('Search by name or query…').fill('forklift')
await page.waitForTimeout(600) // debounce
await page.waitForLoadState('networkidle')
rowCount = await page.locator('table tbody tr').count()
const rowTexts = await page.locator('table tbody tr').allInnerTexts()
console.log('Jobs: rows after searching "forklift" =', rowCount)
console.log('Jobs: all rows mention forklift =', rowTexts.every((t) => t.toLowerCase().includes('forklift')))

await page.getByPlaceholder('Search by name or query…').fill('')
await page.waitForTimeout(600)

await page.screenshot({ path: `${process.cwd()}/verify-jobs.png`, fullPage: true })

// --- Assets page: pick a job with assets, check download button ---
await page.goto('http://localhost:5173/jobs', { waitUntil: 'networkidle' })
await page.waitForTimeout(400)
// Click first job row with "Completed" status to open its detail, then go to assets
const link = page.locator('table tbody tr a, table tbody tr').first()
await page.locator('table tbody tr').first().click()
await page.waitForLoadState('networkidle')
await page.waitForTimeout(500)

const assetsLink = page.getByRole('link', { name: /assets/i })
if (await assetsLink.count() > 0) {
  await assetsLink.first().click()
  await page.waitForLoadState('networkidle')
  await page.waitForTimeout(700)

  const assetCount = await page.locator('img[alt]').count()
  console.log('Assets: image count on page =', assetCount)

  // Hover first asset card to reveal download button
  const firstCard = page.locator('.group').first()
  await firstCard.hover()
  await page.waitForTimeout(300)
  const downloadBtn = firstCard.locator('button[title="Download"]')
  console.log('Assets: download button visible on hover =', await downloadBtn.count() > 0)

  await page.screenshot({ path: `${process.cwd()}/verify-assets.png`, fullPage: true })
} else {
  console.log('No "View assets" link found on this job detail page')
}

await browser.close()
