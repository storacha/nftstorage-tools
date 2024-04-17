/** @param {string} key */
export const mustGetEnv = key => {
  const val = process.env[key]
  if (!val) throw new Error(`missing environment variable: ${key}`)
  return val
}