'use strict'

const figgyPudding = require('figgy-pudding')
const index = require('./lib/entry-index')
const memo = require('./lib/memoization')
const write = require('./lib/content/write')
const to = require('mississippi').to

const PutOpts = figgyPudding({
  algorithms: {
    default: ['sha512']
  },
  integrity: {},
  existKey: {},
  existIntegrity: {},
  memoize: {},
  metadata: {},
  pickAlgorithm: {},
  size: {},
  tmpPrefix: {},
  uid: {},
  gid: {},
  single: {},
  sep: {},
  strict: {}
})

module.exports = putData
function putData (cache, key, data, opts) {
  opts = PutOpts(opts)
  return write(cache, data, opts).then(res => {
    return index.insert(
      cache, key, res.integrity, opts.concat({size: res.size})
    ).then(entry => {
      if (opts.memoize) {
        memo.put(cache, entry, data, opts)
      }
      return res.integrity
    })
  })
}

module.exports.stream = putStream
function putStream (cache, key, opts) {
  opts = PutOpts(opts)
  let integrity
  let size
  const contentStream = write.stream(
    cache, opts
  ).on('integrity', int => {
    integrity = int
  }).on('size', s => {
    size = s
  })
  let memoData
  let memoTotal = 0
  const stream = to((chunk, enc, cb) => {
    contentStream.write(chunk, enc, () => {
      if (opts.memoize) {
        if (!memoData) { memoData = [] }
        memoData.push(chunk)
        memoTotal += chunk.length
      }
      cb()
    })
  }, cb => {
    contentStream.end(() => {
      // avoid growing index arbitrary
      const sameKey = opts.existKey && opts.existKey === key;
      // not same key or same integrity => must update index entry
      // technically the key can't be different.
      if (!sameKey || !(opts.existIntegrity && opts.existIntegrity === integrity.toString())) {
        index.insert(cache, key, integrity, opts.concat({size})).then(entry => {
          if (opts.memoize) {
            memo.put(cache, entry, Buffer.concat(memoData, memoTotal), opts)
          }
          stream.emit('integrity', integrity)
          cb()
        })
      } else {
        process.nextTick(() => {
          stream.emit('integrity', integrity)
          cb()
        })
      }
    })
  })
  let erred = false
  stream.once('error', err => {
    if (erred) { return }
    erred = true
    contentStream.emit('error', err)
  })
  contentStream.once('error', err => {
    if (erred) { return }
    erred = true
    stream.emit('error', err)
  })
  return stream
}
