/**
 * Usage: NODE_TLS_REJECT_UNAUTHORIZED=0 node count-filecoin-size-monthly.mjs
 */
import fs from 'node:fs'
import dotenv from 'dotenv'
import pg from 'pg'
import bytes from 'bytes'
import { mustGetEnv } from './utils.mjs'

dotenv.config()

const TB = 1024 * 1024 * 1024 * 1024

/**
 * @typedef {{ value: string, collected_at: string }} Metric
 */

/**
 * @param {import('pg').PoolClient} db
 * @returns {Promise<Array<Metric>>}
 */
const fetchStoredBytesActive = async (db) => {
  const { rows } = await db.query(`select value, collected_at from cargo.metrics_log where name = 'dagcargo_project_stored_bytes_active' and dimensions @> '{"project", "nft.storage"}' order by collected_at asc`)
  return rows
}

/**
 * @typedef {number} Year
 * @typedef {number} Month
 */

/**
 * @template T
 * @typedef {Record<Year, Record<Month, T>>} MonthlyItems
 */

/**
 * @typedef {MonthlyItems<number>} MonthlyTotals
 */

/** @param {number} m */
const monthToString = m => ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'][m]

/** @param {number} m */
const monthToPaddedString = m => (m + 1).toString().padStart(2, '0')

async function main () {
  const connectionString = mustGetEnv('PROD_RO_DATABASE_CONNECTION')
  const pool = new pg.Pool({ connectionString })

  /** @param {MonthlyTotals} totals */
  const logSummary = totals => {
    const yearLines = []
    const years = Object.keys(totals).map(k => parseInt(k)).sort((a, b) => a - b)
    for (const yr of years) {
      const months = Object.keys(totals[yr]).map(k => parseInt(k)).sort((a, b) => a - b)
      let bytesTotal = 0
      const lines = []
      for (const m of months) {
        bytesTotal += totals[yr][m]
        lines.push(`  ${monthToString(m)}: ${bytes(totals[yr][m])}`)
      }
      yearLines.push(`${yr} ${bytes(bytesTotal)}`)
      yearLines.push(lines.join('\n'))
    }
    console.log(yearLines.join('\n'))
  }

  let db
  try {
    db = await pool.connect()
    const metrics = await fetchStoredBytesActive(db)

    let currYear, currMonth, prevYear, prevMonth
    let prevMonthTotal
    /** @type {MonthlyTotals} */
    const totals = {}
    for (const m of metrics) {
      const collectedAt = new Date(m.collected_at)
      const year = collectedAt.getFullYear()
      const month = collectedAt.getMonth()
      totals[year] = totals[year] ?? {}
      totals[year][month] = Number(m.value) - (prevMonthTotal ?? 0)

      if (currYear == null && currMonth == null) {
        currYear = year
        currMonth = month
      }

      if (month !== currMonth) {
        prevYear = currYear
        prevMonth = currMonth
        currYear = year
        currMonth = month
        prevMonthTotal = Number(m.value)
      }
    }
    logSummary(totals)

    const columns = 'Month,Total Data (TiB),New Data (TiB)'
    const csv = [columns]

    let totalData = 0
    for (const year of Object.keys(totals).map(k => parseInt(k)).sort((a, b) => a - b)) {
      for (const month of Object.keys(totals[year]).map(k => parseInt(k)).sort((a, b) => a - b)) {
        totalData += totals[year][month]
        csv.push([
          `${year}-${monthToString(month)}`,
          (totalData/TB).toFixed(2),
          (totals[year][month]/TB).toFixed(2)
        ].join(','))
      }
    }

    await fs.promises.writeFile('./out/filecoin.csv', csv.join('\n'))
  } finally {
    db && db.release()
    await pool.end()
  }
}

main()
