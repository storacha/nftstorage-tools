import fs from 'node:fs'

/**
 * @template S
 * @param {{ decode: (d: Uint8Array) => S, path: string }} conf
 */
export const load = async ({ decode, path }) => {
  try {
    return decode(await fs.promises.readFile(path))
  } catch (err) {
    if (err.code !== 'ENOENT') throw err
  }
}

/**
 * @template S
 * @param {{ encode: (s: S) => any, path: string }} conf
 * @param {S} state
 */
export const store = ({ encode, path }, state) => fs.promises.writeFile(path, encode(state))
