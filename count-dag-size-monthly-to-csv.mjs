import fs from 'node:fs'
import * as dagJSON from '@ipld/dag-json'
import * as State from './lib/state.mjs'

const TB = 1024 * 1024 * 1024 * 1024
const statePath = `${import.meta.dirname}/out/count-dag-size-monthly.state.json`

/** @param {Uint8Array} data */
const decodeState = data => {
  const raw = dagJSON.decode(data)
  return /** @type {import('./count-dag-size-monthly.mjs').State} */ ({ ...raw, currentID: BigInt(raw.currentID) })
}

const state = await State.load({ path: statePath, decode: decodeState })
if (!state) throw new Error('missing state')

/** @param {number} m */
const monthToString = m => (m + 1).toString().padStart(2, '0')

const columns = 'Month,Total Data (TiB),New Data (TiB),Total Uploads,New Uploads'
const real = [columns]
const adjusted = [columns]

let totalDataReal = 0
let totalDataAdjusted = 0
let totalUploadsReal = 0
let totalUploadsAdjusted = 0
for (const year of Object.keys(state.totals).map(k => parseInt(k)).sort((a, b) => a - b)) {
  for (const month of Object.keys(state.totals[year]).map(k => parseInt(k)).sort((a, b) => a - b)) {
    totalDataReal += state.totals[year][month].real.bytes
    totalUploadsReal += state.totals[year][month].real.count
    real.push([
      `${year}-${monthToString(month)}`,
      (totalDataReal/TB).toFixed(2),
      (state.totals[year][month].real.bytes/TB).toFixed(2),
      totalUploadsReal,
      state.totals[year][month].real.count
    ].join(','))

    totalDataAdjusted += state.totals[year][month].adjusted.bytes
    totalUploadsAdjusted += state.totals[year][month].adjusted.count
    adjusted.push([
      `${year}-${monthToString(month)}`,
      (totalDataAdjusted/TB).toFixed(2),
      (state.totals[year][month].adjusted.bytes/TB).toFixed(2),
      totalUploadsAdjusted,
      state.totals[year][month].adjusted.count
    ].join(','))
  }
}

await Promise.all([
  fs.promises.writeFile('./out/real.csv', real.join('\n')),
  fs.promises.writeFile('./out/adjusted.csv', adjusted.join('\n'))
])
