import fs from 'node:fs'
import { Writable } from 'node:stream'
import dotenv from 'dotenv'
import { Stringify } from 'ndjson-web'
import retry from 'p-retry'
import { mustGetEnv } from './utils.mjs'

dotenv.config()

/**
 * @typedef {{ cursor: string }} State
 */

const statePath = './out/sync-denylist.state.json'

/** @returns {Promise<State>} */
async function loadState () {
  try {
    const data = JSON.parse(await fs.promises.readFile(statePath, 'utf-8'))
    return { cursor: data.cursor }
  } catch (err) {
    if (err.code !== 'ENOENT') throw err
    return { cursor: '' }
  }
}

/** @param {State} state */
async function saveState (state) {
  const data = { cursor: state.cursor }
  await fs.promises.writeFile(statePath, JSON.stringify(data))
}

const accountID = mustGetEnv('CF_ACCOUNT_ID')
const namespaceID = mustGetEnv('CF_DENYLIST_NAMESPACE_ID')
const apiToken = mustGetEnv('CF_API_TOKEN')

const state = await loadState()

const source = new ReadableStream({
  async pull (controller) {
    const url = new URL(`https://api.cloudflare.com/client/v4/accounts/${accountID}/storage/kv/namespaces/${namespaceID}/keys`)
    url.searchParams.set('cursor', state.cursor)
    const page = await retry(async () => {
      const res = await fetch(url.toString(), {
        headers: {
          Authorization: `bearer ${apiToken}`,
          'Content-Type': 'application/json'
        }
      })
      if (!res.ok) throw new Error(`failed to fetch page: ${res.status}`)
      return await res.json()
    }, { onFailedAttempt: console.error })
    
    for (const { name } of page.result) {
      controller.enqueue(name)
    }
    if (!page.result_info.cursor) {
      return controller.close()
    }
    state.cursor = page.result_info.cursor
    await saveState(state)
  }
})

await source
  .pipeThrough(new Stringify())
  .pipeTo(Writable.toWeb(fs.createWriteStream('./out/denylist.json', { flags: state.cursor ? 'a' : 'w' })))
