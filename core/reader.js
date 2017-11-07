'use strict'

const EventEmitter = require('events')
const uuid = require('uuid/v4')
const redisKeys = require('./redis-keys')
const redisClient = require('./redis-client')
const logger = require('../logger')

const read = Symbol('read')
const sLogger = Symbol('logger')

class Reader extends EventEmitter {
  /**
   * @param {string} queueName
   * @param {object} config
   */
  constructor (queueName, config) {
    super()
    this.readerId = uuid()
    this.queueName = queueName
    this.keys = redisKeys.getKeys(queueName)
    this.client = redisClient.getNewInstance(config)
    this.isTest = process.env.NODE_ENV === 'test'
    this[sLogger] = logger.getNewInstance(`${this.queueName}:${this.readerId}`, config.log)
  }

  /**
   * @param {string} key
   * @param {Boolean} flush
   * @param {function} callback
   */
  [read] (key, flush, callback) {
    let messages = null

    const onRemove = (err) => {
      if (err) callback(err)
      else callback(null, messages)
    }

    const onRange = (err, range) => {
      if (err) callback(err)
      else {
        messages = range
        this[sLogger].debug(`Found [${messages.length}] messages`)

        if (flush === true) this.client.ltrim(key, messages.length, -1, onRemove.bind(this))
        else callback(null, messages)
      }
    }

    const onLength = (err, len) => {
      if (err) callback(err)
      else {
        this[sLogger].debug(`Total length of [${key}] is [${len}]`)
        this.client.lrange(key, -len, -1, onRange.bind(this))
      }
    }

    this.client.llen(key, onLength.bind(this))
  }

  readDeadQueue (callback) {
    this[read](this.keys.keyQueueNameDead, true, callback)
  }
}

module.exports = Reader
