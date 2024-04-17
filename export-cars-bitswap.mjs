/**
 * Usage: NODE_TLS_REJECT_UNAUTHORIZED=0 node export-cars.mjs
 */
import fs from 'node:fs'
import { Readable, Writable } from 'node:stream'
import dotenv from 'dotenv'
import pg from 'pg'
import { NFTStorage } from 'nft.storage'
import { CARReaderStream, CARWriterStream } from 'carstream'
import { getLibp2p, fromNetwork } from 'dagula/p2p.js'
import * as Link from 'multiformats/link'
import bytes from 'bytes'
import { TimeoutController } from 'timeout-abort-controller'
import { HashingLinkIndexer } from 'linkdex/hashing-indexer.js'

dotenv.config()

/**
 * @typedef {{
 *   id: number
 *   user_id: number
 *   key_id: number
 *   source_cid: string
 *   content_cid: string
 *   mime_type?: string
 *   type: 'Blob'|'Car'|'Multipart'|'Nft'|'Remote'
 *   name?: string
 *   files?: Array<{ name: string, type: string }>
 *   origins?: any
 *   meta?: any
 *   inserted_at: string
 *   updated_at: string
 *   deleted_at?: string
 *   backup_urls?: string[]
 * }} Upload
 * @typedef {{
 *   upload: Upload
 *   path: string
 * }} LocalUpload
 */

const PEERS = [
  '/ip4/212.6.53.91/tcp/8888/p2p/12D3KooWFzZYEChh6Dsy4jHyTopK42oRrk41WVfnpMG5r8eoXa6u',
  '/ip4/212.6.53.53/tcp/8888/p2p/12D3KooWJqxfjvbDkKByJQeT5urmeFBavNhNvidsAeqjWr6hf7jT',
  '/ip4/212.6.53.51/tcp/8888/p2p/12D3KooWRPfVRXRzEsSrJVhcu616vniJF8zQbuBN87skKdh93CbM'
]

const LIST_NFTS = `SELECT * FROM upload WHERE type = 'Nft' OFFSET $1 LIMIT $2`

async function main () {
  const connectionString = mustGetEnv('PROD_RO_DATABASE_CONNECTION')
  const token = mustGetEnv('NFT_STORAGE_TOKEN')

  /** @type {pg.Pool} */
  const pool = new pg.Pool({ connectionString })

  try {
    await getUploadReader(pool)
      .pipeThrough(getUploadFilter())
      .pipeThrough(getCompleteFilter())
      .pipeThrough(getContentExtractor())
      .pipeThrough(getContentStorer(token))
      .pipeTo(new WritableStream({
        async write (item) {
          await setComplete(item.upload.content_cid)
          await fs.promises.rm(item.path)
        }
      }))
    console.log('✅ done')
  } finally {
    await pool.end()
  }
}

/** @param {string} key */
const mustGetEnv = key => {
  const val = process.env[key]
  if (!val) throw new Error(`missing environment variable: ${key}`)
  return val
}

/** @param {string} root */
const isComplete = async root => {
  try {
    const stat = await fs.promises.stat(`./out/completed/${root}`)
    return stat.isFile()
  } catch {
    return false
  }
}

/** @param {string} root */
const setComplete = async root => {
  await fs.promises.mkdir('./out/completed', { recursive: true })
  await fs.promises.writeFile(`./out/completed/${root}`, '')
}

/** @param {pg.Pool} pg */
const getUploadReader = (pg) => {
  let offset = 0
  const limit = 1000
  /** @type {ReadableStream<Upload>} */
  const readable = new ReadableStream({
    async pull (controller) {
      let db
      try {
        console.log(`reading uploads ${offset}-${offset+limit}`)
        db = await pg.connect()
        const results = await db.query(LIST_NFTS, [offset, limit])
        if (!results.rows.length) return controller.close()
        const rows = results.rows.filter(r => (r.backup_urls ?? []).length === 0)
        for (const row of rows) controller.enqueue(row)
        offset += limit
      } finally {
        db && db.release()
      }
    }
  })
  return readable
}

const getUploadFilter = () => {
  /** @type {TransformStream<Upload, Upload>} */
  return new TransformStream({
    transform (upload, controller) {
      if ((upload.backup_urls ?? []).length) return
      controller.enqueue(upload)
    }
  })
}

const getCompleteFilter = () => {
  /** @type {TransformStream<Upload, Upload>} */
  return new TransformStream({
    async transform (upload, controller) {
      const complete = await isComplete(upload.content_cid)
      if (complete) {
        return console.log(`✅ previously stored ${upload.content_cid}`)
      }
      controller.enqueue(upload)
    }
  })
}

const getContentExtractor = () => {
  /** @type {TransformStream<Upload, LocalUpload>} */
  const transform = new TransformStream({
    async start () {
      await fs.promises.mkdir('./out/contents', { recursive: true })
    },
    async transform (upload, controller) {
      const peers = shuffle([...PEERS])
      for (const peer of peers) {
        const destPath = `./out/contents/${upload.content_cid}.car`
        const root = Link.parse(upload.content_cid)
        try {
          await exportDAG(peer, root)
            .pipeThrough(new CARWriterStream([root]))
            .pipeTo(Writable.toWeb(fs.createWriteStream(destPath)))
          await verifyDAG(root, destPath)
          controller.enqueue({ path: destPath, upload })
          break
        } catch (err) {
          console.error(`failed to bitswap ${root} from ${peer}: ${err.message}`)
        }
      }
    }
  })
  return transform
}

/**
 * @template T
 * @param {Array<T>} array
 */
const shuffle = array => {
  let currentIndex = array.length, randomIndex

  // While there remain elements to shuffle.
  while (currentIndex > 0) {
    // Pick a remaining element.
    randomIndex = Math.floor(Math.random() * currentIndex)
    currentIndex--

    // And swap it with the current element.
    [array[currentIndex], array[randomIndex]] = [
      array[randomIndex], array[currentIndex]]
  }

  return array
}

/**
 * @param {import('multiformats').UnknownLink} root
 * @param {string} path
 */
const verifyDAG = async (root, path) => {
  console.log(`verifying ${root} in ${path}`)
  const indexer = new HashingLinkIndexer()
  try {
    const carReader = new CARReaderStream()
    await Readable.toWeb(fs.createReadStream(path))
      // @ts-expect-error
      .pipeThrough(carReader)
      .pipeTo(new WritableStream({
        async write (block) {
          // @ts-expect-error
          await indexer.decodeAndIndex(block)
        }
      }))
    const header = await carReader.getHeader()
    if (header.roots[0]?.toString() !== root.toString()) {
      throw new Error(`root CID mismatch ${header.roots[0]} != ${root}`)
    }
  } catch (err) {
    throw new Error(`failed indexing: ${err.message}`, { cause: err })
  }
  try {
    const report = indexer.report()
    // console.log(report)
    if (report.structure !== 'Complete') {
      throw Object.assign(new Error(`non-complete DAG: ${report.structure}`), report)
    }
  } catch (err) {
    throw new Error(`failed verification: ${err.message}`, { cause: err })
  }
  console.log(`🏅 successfully verified ${path}`)
}

/**
 * @param {string} peer
 * @param {import('multiformats').UnknownLink} content
 */
const exportDAG = (peer, content) => {
  const timeout = new TimeoutController(5000)
  let totalBytes = 0
  const interval = setInterval(() => console.log(`exported ${bytes(totalBytes)} of ${content}`), 10000)
  let iterator
  /** @type {ReadableStream<import('dagula').Block>} */
  const readable = new ReadableStream({
    async start () {
      const libp2p = await getLibp2p()
      const dagula = await fromNetwork(libp2p, { peer })
      iterator = dagula.get(content, { order: 'dfs', signal: timeout.signal })
      console.log(`exporting ${content} from ${peer}`)
    },
    async pull (controller) {
      try {
        const { done, value } = await iterator.next()
        timeout.reset()
        if (done) {
          timeout.clear()
          clearInterval(interval)
          console.log(`✅⤵️ successfully exported ${content} from ${peer}`)
          return controller.close()
        }
        totalBytes += value.bytes.length
        controller.enqueue(value)
      } catch (err) {
        timeout.clear()
        clearInterval(interval)
        throw err
      }
    },
    cancel () {
      timeout.clear()
      clearInterval(interval)
    }
  })
  return readable
}

/** @param {string} token */
const getContentStorer = token => {
  const storage = new NFTStorage({ token })

  /** @type {TransformStream<LocalUpload, LocalUpload>} */
  const transform = new TransformStream({
    async transform ({ path, upload }, controller) {
      console.log(`storing ${path}`)
      const data = await fs.promises.readFile(path)
      const blob = new Blob([data])
      await storage.storeCar(blob, {
        onStoredChunk: (size) => {
          console.log(`storing ${upload.content_cid}: ${bytes(size)} of ${bytes(data.length)}`)
        }
      })
      console.log(`✅⤴️ successfully stored ${upload.content_cid}`)
      controller.enqueue({ path, upload })
    }
  })
  return transform
}

main()
