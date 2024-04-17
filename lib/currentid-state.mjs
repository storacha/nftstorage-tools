import * as dagJSON from '@ipld/dag-json'

/** @typedef {{ currentID: bigint }} State */

/** @param {Uint8Array} data */
export const decode = data => {
  const raw = dagJSON.decode(data)
  return /** @type {State} */ ({ currentID: BigInt(raw.currentID) })
}

/** @param {State} state */
export const encode = state => dagJSON.encode({ currentID: String(state.currentID) })

export const init = () => ({ currentID: 0n })
