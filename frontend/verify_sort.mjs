import { chromium } from 'playwright'

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1400, height: 900 } })
page.on('pageerror', (e) => console.log('[pageerror]', e.message))

await page.goto('http://localhost:5173/jobs', { waitUntil: 'networkidle' })
await page.waitForTimeout(500)

const rows = await page.locator('table tbody tr').all()
console.log('row count:', rows.length)
for (const row of rows.slice(0, 6)) {
  const text = await row.innerText()
  console.log('---', text.replace(/\n/g, ' | '))
}

await page.screenshot({ path: `${process.cwd()}/verify-sort.png`, fullPage: true })
await browser.close()
