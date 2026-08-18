import { chromium } from 'playwright'

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1400, height: 1000 } })
page.on('pageerror', (e) => console.log('[pageerror]', e.message))
page.on('console', (m) => { if (m.type() === 'error') console.log('[console.error]', m.text()) })

await page.goto('http://localhost:5173/jobs/1345fe77-f931-4b4d-8917-6479e452742e/assets', { waitUntil: 'networkidle' })
await page.waitForTimeout(2500) // let all blob image fetches resolve

let cardCount = await page.locator('.group').count()
let loadedImgCount = await page.locator('.group img').count()
console.log('Assets: cards =', cardCount, ' loaded <img> =', loadedImgCount)

// Get one asset's actual source_item_id substring from the API to search for something that should match exactly one card
const resp = await page.request.get('http://localhost:8000/api/v1/jobs/1345fe77-f931-4b4d-8917-6479e452742e/assets?limit=1', {
  headers: { 'X-API-Key': 'dev-local-key' },
})
const body = await resp.json()
const targetId = body.items[0].source_item_id
console.log('Targeting unique search term:', targetId)

await page.getByPlaceholder('Search by filename or dataset…').fill(targetId)
await page.waitForTimeout(600)
await page.waitForLoadState('networkidle')
await page.waitForTimeout(500)
const filteredCount = await page.locator('.group').count()
console.log('Assets: cards after searching unique source_item_id =', filteredCount)

await page.screenshot({ path: `${process.cwd()}/verify-assets3.png`, fullPage: true })
await browser.close()
