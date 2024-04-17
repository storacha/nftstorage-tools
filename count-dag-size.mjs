/**
 * Usage: NODE_TLS_REJECT_UNAUTHORIZED=0 node count-dag-size.mjs
 */
import fs from 'node:fs'
import dotenv from 'dotenv'
import pg from 'pg'
import ora from 'ora'
import bytes from 'bytes'
import { readDenyList, isDenyListed } from './lib/denylist.mjs'
import { mustGetEnv } from './utils.mjs'

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
 * @param {import('pg').PoolClient} db
 * @param {bigint} id
 * @param {number} limit
 * @returns {Promise<Array<{ source_cid: string, content_cid: string }>>}
 */
async function fetchUploadsAfter (db, id, limit) {
  const { rows } = await db.query('SELECT id, source_cid, content_cid FROM upload WHERE id >= $1 ORDER BY id ASC LIMIT $2', [id.toString(), limit.toString()])
  return rows
}

/**
 * @param {import('pg').PoolClient} db
 * @param {string[]} cids
 */
async function fetchTotalDagSize (db, cids) {
  const { rows } = await db.query(COUNT_DAG_SIZE.replace('$1', `'${cids.join("','")}'`))
  return BigInt(rows[0].sum)
}

/**
 * @typedef {{ currentID: bigint, size: bigint, count: number }} State
 */

const statePath = './out/count-dag-size.state.json'

/** @returns {Promise<State>} */
async function loadState () {
  try {
    const data = JSON.parse(await fs.promises.readFile(statePath, 'utf-8'))
    return { currentID: BigInt(data.currentID), size: BigInt(data.size), count: data.count }
  } catch (err) {
    if (err.code !== 'ENOENT') throw err
    return { currentID: 0n, size: 0n, count: 0 }
  }
}

/** @param {State} state */
async function saveState (state) {
  const data = { currentID: state.currentID.toString(), size: state.size.toString(), count: state.count }
  await fs.promises.writeFile(statePath, JSON.stringify(data))
}

async function main () {
  const connectionString = mustGetEnv('PROD_RO_DATABASE_CONNECTION')
  const state = await loadState()
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
  const updateSpinner = () => spinner.text = `${state.currentID.toLocaleString()} of ${range.max.toLocaleString()} (${state.count.toLocaleString()} - ${bytes(Number(state.size))})`
  updateSpinner()

  try {
    while (state.currentID < range.max) {
      try {
        db = await pool.connect()
        const uploads = await fetchUploadsAfter(db, state.currentID, PAGE_SIZE)
        const cids = uploads.filter(u => !isDenyListed(denylist, u.source_cid)).map(u => u.content_cid)
        // const cids = uploads.map(u => u.content_cid)
        if (cids.length) {
          const size = await fetchTotalDagSize(db, cids)
          state.size += size
        }
        state.count += cids.length
        state.currentID = state.currentID + BigInt(PAGE_SIZE)
        await saveState(state)
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
