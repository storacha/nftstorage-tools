import fs from 'node:fs'
import { Readable } from 'node:stream'
import { Parse } from 'ndjson-web'
import { sha256 } from 'multiformats/hashes/sha2'

export async function readDenyList () {
  const list = new Set()
  const source = /** @type {ReadableStream<Uint8Array>} */
    (Readable.toWeb(fs.createReadStream(`${import.meta.dirname}/../out/denylist.json`)))
  await source
    .pipeThrough(new Parse())
    .pipeTo(new WritableStream({
      write (anchor) {
        list.add(anchor)
      }
    }))
  return list
}

/**
 * @param {Set<string>} denylist
 * @param {string} cid
 */
export function isDenyListed (denylist, cid) {
  const hash = sha256.encode(Buffer.from(`${cid}/`))
  if (hash instanceof Promise) throw new Error('unexpected async sha256 hasher')
  const exists = denylist.has(Buffer.from(hash).toString('hex'))
  // if (exists) console.log(`\nfound on denylist: ${cid}, (${Buffer.from(hash).toString('hex')})`)
  return exists
}
