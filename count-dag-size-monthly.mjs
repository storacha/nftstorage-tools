/**
 * Usage: NODE_TLS_REJECT_UNAUTHORIZED=0 node count-dag-size-monthly.mjs
 */
import fs from 'node:fs'
import dotenv from 'dotenv'
import pg from 'pg'
import ora from 'ora'
import bytes from 'bytes'
import * as dagJSON from '@ipld/dag-json'
import { readDenyList, isDenyListed } from './lib/denylist.mjs'
import { mustGetEnv } from './utils.mjs'
import * as State from './lib/state.mjs'

dotenv.config()

const COUNT_DAG_SIZE = 'SELECT SUM(size_actual) FROM cargo.dags WHERE cid_v1 IN ($1)'
const PAGE_SIZE = 10000

/** @param {import('pg').PoolClient} db */
async function fetchUploadIdRange (db) {
  const { rows } = await db.query('SELECT MIN(id), MAX(id) FROM upload')
  if (!rows.length) throw new Error('no rows returned fetching min/max ID')
  return { min: BigInt(rows[0].min), max: BigInt(rows[0].max) }
}

/**
 * @typedef {{ source_cid: string, content_cid: string, inserted_at: string }} Upload
 */

/**
 * @param {import('pg').PoolClient} db
 * @param {bigint} id
 * @param {number} limit
 * @returns {Promise<Array<Upload>>}
 */
async function fetchUploadsAfter (db, id, limit) {
  const { rows } = await db.query('SELECT id, source_cid, content_cid, inserted_at FROM upload WHERE id >= $1 ORDER BY id ASC LIMIT $2', [id.toString(), limit.toString()])
  return rows
}

/**
 * @param {import('pg').PoolClient} db
 * @param {string[]} cids
 */
async function fetchTotalDagSize (db, cids) {
  const { rows } = await db.query(COUNT_DAG_SIZE.replace('$1', `'${cids.join("','")}'`))
  return Number(rows[0].sum)
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
 * @typedef {MonthlyItems<Upload[]>} MonthlyUploads
 */

/**
 * @typedef {{ bytes: number, count: number }} Total
 * @typedef {MonthlyItems<{ real: Total, adjusted: Total }>} MonthlyTotals
 */

/**
 * @typedef {{ currentID: bigint, totals: MonthlyTotals }} State
 */

const statePath = `${import.meta.dirname}/out/count-dag-size-monthly.state.json`

/** @param {Uint8Array} data */
const decodeState = data => {
  const raw = dagJSON.decode(data)
  return /** @type {State} */ ({ ...raw, currentID: BigInt(raw.currentID) })
}

/** @param {State} state */
const encodeState = state => dagJSON.encode({ ...state, currentID: String(state.currentID) })

/** @param {number} m */
const monthToString = m => ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'][m]

async function main () {
  const connectionString = mustGetEnv('PROD_RO_DATABASE_CONNECTION')
  const state = (await State.load({ path: statePath, decode: decodeState })) ?? { currentID: 0n, totals: {} }
  const denylist = await readDenyList()
  console.log(`Denylist size: ${denylist.size}`)

  const pool = new pg.Pool({ connectionString })

  let range
  let db
  try {
    db = await pool.connect()
    range = await fetchUploadIdRange(db)
  } finally {
    db && db.release()
  }

  const spinner = ora().start()
  const updateSpinner = () => {
    const yearLines = []
    const years = Object.keys(state.totals).map(k => parseInt(k)).sort((a, b) => a - b)
    for (const yr of years) {
      const months = Object.keys(state.totals[yr]).map(k => parseInt(k)).sort((a, b) => a - b)
      let bytesTotal = 0
      let countTotal = 0
      const lines = []
      for (const m of months) {
        bytesTotal += state.totals[yr][m].real.bytes
        countTotal += state.totals[yr][m].real.count
        lines.push(`  ${monthToString(m)}: ${bytes(state.totals[yr][m].real.bytes)} (${state.totals[yr][m].real.count.toLocaleString()})`)
      }
      yearLines.push(`${yr} ${bytes(bytesTotal)} (${countTotal.toLocaleString()})`)
      yearLines.push(lines.join('\n'))
    }
    spinner.text = `${state.currentID.toLocaleString()} of ${range.max.toLocaleString()}\n\n${yearLines.join('\n')}`
  }
  updateSpinner()

  try {
    while (state.currentID < range.max) {
      try {
        db = await pool.connect()
        const uploads = await fetchUploadsAfter(db, state.currentID, PAGE_SIZE)
        
        /** @type {MonthlyUploads} */
        const monthlyUploads = {}
        for (const u of uploads) {
          const insertedAt = new Date(u.inserted_at)
          const year = insertedAt.getFullYear()
          const month = insertedAt.getMonth()
          monthlyUploads[year] = monthlyUploads[year] ?? {}
          monthlyUploads[year][month] = monthlyUploads[year][month] ?? []
          monthlyUploads[year][month].push(u)
        }

        for (const year of Object.keys(monthlyUploads)) {
          for (const month of Object.keys(monthlyUploads[year])) {
            const uploads = monthlyUploads[year][month]
            state.totals[year] = state.totals[year] ?? {}
            state.totals[year][month] = state.totals[year][month] ?? { real: { bytes: 0, count: 0 }, adjusted: { bytes: 0, count: 0 } }

            await Promise.all([
              (async () => {
                state.totals[year][month].real.bytes += await fetchTotalDagSize(db, uploads.map(u => u.content_cid))
                state.totals[year][month].real.count += uploads.length
              })(),
              (async () => {
                const denyCIDs = uploads.filter(u => !isDenyListed(denylist, u.source_cid)).map(u => u.content_cid)
                if (denyCIDs.length) {
                  state.totals[year][month].adjusted.bytes += await fetchTotalDagSize(db, denyCIDs)
                  state.totals[year][month].adjusted.count += denyCIDs.length
                }
              })()
            ])
          }
        }
        state.currentID = state.currentID + BigInt(PAGE_SIZE)
        await State.store({ path: statePath, encode: encodeState }, state)
        updateSpinner()
      } finally {
        db && db.release()
      }
    }
    updateSpinner()
    spinner.stopAndPersist()
  } catch (err) {
    spinner.fail(err.stack)
  } finally {
    await pool.end()
  }
}

main()
