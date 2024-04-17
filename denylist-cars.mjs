/**
 * Usage: NODE_TLS_REJECT_UNAUTHORIZED=0 node denylist-cars.mjs
 */
// Iterate over user uploads
// If denied
//     collect from dynamo blocks_cars_position table
import fs from 'node:fs'
import dotenv from 'dotenv'
import pg from 'pg'
import ora from 'ora'
import { readDenyList, isDenyListed } from './lib/denylist.mjs'
import { Writable } from 'node:stream'
import { Stringify } from 'ndjson-web'
import retry from 'p-retry'
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import { base58btc } from 'multiformats/bases/base58'
import * as Link from 'multiformats/link'
import map from 'p-map'
import * as State from './lib/state.mjs'
import * as CurrentIDState from './lib/currentid-state.mjs'
import { mustGetEnv } from './utils.mjs'

dotenv.config()

const PAGE_SIZE = 10000
const BLOCKS_CARS_TABLE = mustGetEnv('DYNAMO_BLOCKS_CARS_POSITION_TABLE')

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
 * @returns {Promise<Array<{ id: string, user_id: string, source_cid: string, content_cid: string, backup_urls: string[] }>>}
 */
async function fetchUploadsAfter (db, id, limit) {
  const { rows } = await db.query('SELECT id, user_id, source_cid, content_cid FROM upload WHERE id >= $1 ORDER BY id ASC LIMIT $2', [id, limit].map(String))
  return rows
}

/**
 * @param {import('pg').PoolClient} db
 * @param {string[]} cids
 */
async function fetchTotalDagSize (db, cids) {
  const { rows } = await db.query('SELECT SUM(size_actual) FROM cargo.dags WHERE cid_v1 IN ($1)'.replace('$1', `'${cids.join("','")}'`))
  return Number(rows[0].sum)
}

/**
 * @param {DynamoDBClient} dynamo
 * @param {import('multiformats').Link} root
 */
const fetchBucketKeys = async (dynamo, root) => {
  const keys = []
  let cursor
  while (true) {
    const cmd = new QueryCommand({
      TableName: BLOCKS_CARS_TABLE,
      Limit: 1000,
      KeyConditions: {
        blockmultihash: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [{ S: base58btc.encode(root.multihash.bytes) }]
        }
      },
      ExclusiveStartKey: cursor
    })

    const res = await retry(async () => {
      try {
        return await dynamo.send(cmd)
      } catch (err) {
        throw new Error(`failed to list keys: ${root}`, { cause: err })
      }
    }, { onFailedAttempt: console.warn })

    for (const raw of res.Items ?? []) {
      keys.push(raw.carpath.S)
    }

    cursor = res.LastEvaluatedKey
    if (!cursor) break
  }
  return keys
}

const statePath = `${import.meta.dirname}/out/denylist-cars.state.json`
const connectionString = mustGetEnv('PROD_RO_DATABASE_CONNECTION')
const state = (await State.load({ path: statePath, ...CurrentIDState })) ?? CurrentIDState.init()
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
  db = null
}

const dynamo = new DynamoDBClient()
const spinner = ora().start()
const updateSpinner = () => spinner.text = `${state.currentID.toLocaleString()} of ${range.max.toLocaleString()}`
updateSpinner()

const source = new ReadableStream({
  async pull (controller) {
    try {
      db = await pool.connect()

      while (true) {
        if (state.currentID >= range.max) {
          return controller.close()
        }

        const uploads = await fetchUploadsAfter(db, state.currentID, PAGE_SIZE)
        const listedUploads = uploads.filter(u => isDenyListed(denylist, u.source_cid))

        let pushedData = false
        await map(listedUploads, async u => {
          // TODO: use backup_urls, fallback to dynamo
          // TODO: LRU Cache?
          const keys = await fetchBucketKeys(dynamo, Link.parse(u.source_cid))
          if (!keys.length) console.warn(`missing keys for root: ${u.source_cid}`)
          const size = await fetchTotalDagSize(db, [u.content_cid])
          controller.enqueue({ user: u.user_id, upload: u.id, root: u.source_cid, keys, size })
          pushedData = true
        }, { concurrency: 100 })

        state.currentID = state.currentID + BigInt(PAGE_SIZE)
        await State.store({ path: statePath, ...CurrentIDState }, state)
        updateSpinner()

        if (pushedData) break
      }
    } finally {
      db && db.release()
      db = null
    }
  }
})

try {
  await source
    .pipeThrough(new Stringify())
    .pipeTo(Writable.toWeb(fs.createWriteStream('./out/denylist-cars.json', { flags: state.currentID ? 'a' : 'w' })))
  spinner.stopAndPersist()
} catch (err) {
  spinner.fail(err.stack)
} finally {
  await pool.end()
}
