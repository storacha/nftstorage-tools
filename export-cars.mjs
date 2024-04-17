/**
 * Usage: NODE_TLS_REJECT_UNAUTHORIZED=0 node export-cars.mjs
 */
import fs from 'node:fs'
import { Writable } from 'node:stream'
import dotenv from 'dotenv'
import pg from 'pg'
import { S3Client, GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3'
import { NFTStorage } from 'nft.storage'
import { CARWriterStream } from 'carstream'
import { CarIndexedReader } from '@ipld/car'
import { Dagula } from 'dagula'
import * as Link from 'multiformats/link'
import bytes from 'bytes'
import retry from 'p-retry'

dotenv.config()

/**
 * @typedef {{ piece: string, contents: string[], filename: string }} Item
 */

const GET_CARGO_FILE = `SELECT DISTINCT metadata->>'md5hex' || '_' || piece_cid || '.car' AS filename FROM cargo.aggregate_entries JOIN cargo.aggregates USING (aggregate_cid) WHERE cid_v1 = $1;`

async function main () {
  const connectionString = mustGetEnv('PROD_RO_DATABASE_CONNECTION')
  const endpoint = mustGetEnv('R2_ENDPOINT')
  const bucket = mustGetEnv('R2_BUCKET')
  const accessKeyId = mustGetEnv('R2_ACCESS_KEY_ID')
  const secretAccessKey = mustGetEnv('R2_SECRET_ACCESS_KEY')
  const token = mustGetEnv('NFT_STORAGE_TOKEN')

  await getSourceReader()
    .pipeThrough(getFilenameTransformer(connectionString))
    .pipeTo(new WritableStream({
      async write (item) {
        if (await isComplete(item.piece)) {
          return console.log(`${item.piece} has been completed previously`)
        }

        await toReadable([item])
          .pipeThrough(getCargoDownloader(endpoint, bucket, accessKeyId, secretAccessKey))
          .pipeThrough(getContentExtractor())
          .pipeTo(getContentStorer(token))

        await setComplete(item.piece)
        await fs.promises.rm(`./out/${item.filename}`)
      }
    }))  
}

/** @param {string} key */
const mustGetEnv = key => {
  const val = process.env[key]
  if (!val) throw new Error(`missing environment variable: ${key}`)
  return val
}

/**
 * @template T
 * @param {T[]} arr
 */
const toReadable = arr => new ReadableStream({
  pull (controller) {
    const item = arr.shift()
    if (!item) return controller.close()
    controller.enqueue(item)
  }
})

/** @param {string} piece */
const isComplete = async piece => {
  try {
    const stat = await fs.promises.stat(`./out/completed/${piece}`)
    return stat.isFile()
  } catch {
    return false
  }
}

/** @param {string} piece */
const setComplete = async piece => {
  await fs.promises.mkdir('./out/completed', { recursive: true })
  await fs.promises.writeFile(`./out/completed/${piece}`, '')
}

const getSourceReader = () => {
  /** @type {Item[]} */
  const data = []
  /** @type {ReadableStream<Item>} */
  const source = new ReadableStream({
    async start () {
      /** @type {Map<string, string[]>} */
      const pieces = new Map()
      const json = JSON.parse(await fs.promises.readFile('./data_2.json', 'utf8'))
      console.log(`${json.value.length} content CIDs`)
      for (const item of json.value) {
        const cids = pieces.get(item.deals[0].pieceCid) ?? []
        pieces.set(item.deals[0].pieceCid, [...cids, item.cid])
      }
      for (const [k, v] of pieces.entries()) {
        data.push({ piece: k, contents: v, filename: '' })
      }
      console.log(`${data.length} pieces`)
    },
    pull (controller) {
      const item = data.shift()
      if (!item) return controller.close()
      controller.enqueue(item)
    }
  })
  return source
}

/** @param {string} connectionString */
const getFilenameTransformer = (connectionString) => {
  /** @type {pg.Pool} */
  const pool = new pg.Pool({ connectionString })
  /** @type {TransformStream<Item, Item>} */
  const transform = new TransformStream({
    async transform (item, controller) {
      let db
      try {
        db = await pool.connect()
        const results = await db.query(GET_CARGO_FILE, [item.contents[0]])
        const row = results.rows.find(r => r.filename.endsWith(`${item.piece}.car`))
        if (!row) throw new Error(`filename not found for piece: ${item.piece}`)
        controller.enqueue({ ...item, filename: row.filename })
      } finally {
        db && db.release()
      }
    },
    flush () {
      return pool.end()
    }
  })
  return transform
}

/**
 * @param {string} endpoint
 * @param {string} bucket
 * @param {string} accessKeyId
 * @param {string} secretAccessKey
 */
const getCargoDownloader = (endpoint, bucket, accessKeyId, secretAccessKey) => {
  const s3 = new S3Client({
    endpoint,
    region: 'auto',
    credentials: { secretAccessKey, accessKeyId }
  })
  /** @type {TransformStream<Item, Item>} */
  const transform = new TransformStream({
    async transform (item, controller) {
      const destPath = `./out/${item.filename}`

      await retry(async () => {
        let localSize = 0
        // check for exists
        try {
          const stat = await fs.promises.stat(destPath)
          const cmd = new HeadObjectCommand({ Bucket: bucket, Key: item.filename })
          const res = await s3.send(cmd)

          if (res.ContentLength === stat.size) {
            return console.log(`${item.filename} already downloaded!`)
          }

          localSize = stat.size
        } catch {}

        const cmd = new GetObjectCommand({
          Bucket: bucket,
          Key: item.filename,
          Range: `bytes=${localSize}-`
        })
        const res = await s3.send(cmd)
        if (!res.Body) throw new Error('missing body')
        if (res.ContentLength == null) throw new Error('missing content length')

        const totalSize = res.ContentLength + localSize
        const interval = setInterval(async () => {
          try {
            const stat = await fs.promises.stat(destPath)
            console.log(`downloading ${item.filename} ${bytes(stat.size)} of ${bytes(totalSize)}`)
          } catch {}
        }, 30_000)

        try {
          console.log(`starting download of ${item.filename} (bytes=${localSize}-)`)
          await res.Body.transformToWebStream()
            .pipeTo(Writable.toWeb(fs.createWriteStream(destPath, { flags: localSize > 0 ? 'a' : undefined })))
        } finally {
          clearInterval(interval)
        }
      }, {
        onFailedAttempt: console.warn
      })

      controller.enqueue(item)
    }
  })
  return transform
}

const getContentExtractor = () => {
  /** @type {TransformStream<Item, Item>} */
  const transform = new TransformStream({
    async start () {
      await fs.promises.mkdir('./out/contents', { recursive: true })
    },
    async transform (item, controller) {
      console.log(`indexing ${item.filename}`)
      const reader = await CarIndexedReader.fromFile(`./out/${item.filename}`)
      let i = 0
      for (const content of item.contents) {
        const destPath = `./out/contents/${content}.car`
        const root = Link.parse(content)
        console.log(`exporting ${content} (${i + 1} of ${item.contents.length})`)
        await exportDAG(reader, root)
          .pipeThrough(new CARWriterStream([root]))
          .pipeTo(Writable.toWeb(fs.createWriteStream(destPath)))
        i++
      }
      controller.enqueue(item)
    }
  })
  return transform
}

/**
 * @param {CarIndexedReader} reader
 * @param {import('multiformats').UnknownLink} content
 */
const exportDAG = (reader, content) => {
  // @ts-expect-error @ipld/car old multiformats
  const dagula = new Dagula(reader)
  const iterator = dagula.get(content, { order: 'dfs' })
  /** @type {ReadableStream<import('dagula').Block>} */
  const readable = new ReadableStream({
    async pull (controller) {
      const { done, value } = await iterator.next()
      if (done) return controller.close()
      controller.enqueue(value)
    }
  })
  return readable
}

/** @param {string} token */
const getContentStorer = token => {
  const storage = new NFTStorage({ token })

  /** @type {WritableStream<Item, Item>} */
  const writable = new WritableStream({
    async write (item) {
      for (const content of item.contents) {
        const data = await fs.promises.readFile(`./out/contents/${content}.car`)
        const blob = new Blob([data])
        await storage.storeCar(blob, {
          onStoredChunk: (size) => {
            console.log(`storing ${item.piece}/${content} ${bytes(size)} of ${bytes(data.length)}`)
          }
        })
        console.log(`✅ successfully stored ${item.piece}/${content}`)
      }
    }
  })
  return writable
}

main()
