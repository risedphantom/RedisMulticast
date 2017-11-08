'use strict'

const EventEmitter = require('events')
const uuid = require('uuid/v4')
const redisKeys = require('./redis-keys')
const redisClient = require('./redis-client')
const statsFactory = require('./stats')
const logger = require('../logger')

const PRODUCER_LOCK_TTL = 2 // 2 seconds

const produceMessage = Symbol('produceMessage')
const sLogger = Symbol('logger')

class Producer extends EventEmitter {
  /**
   * @param {string} queueName
   * @param {object} config
   */
  constructor (queueName, config) {
    super()
    this.producerId = uuid()
    this.queueName = queueName
    this.keys = redisKeys.getKeys(queueName, null, this.producerId)
    this.client = redisClient.getNewInstance(config)
    this.isTest = process.env.NODE_ENV === 'test'
    this[sLogger] = logger.getNewInstance(`${this.queueName}:${this.producerId}`, config.log)

    const monitorEnabled = !!(config.monitor && config.monitor.enabled)
    if (monitorEnabled) {
      this[sLogger].debug('Turning on statistics collection.')
      this.stats = statsFactory(this, config)
      this.stats.start()
    }
  }

  /**
   * @param {*} body
   * @param {number} ttl
   * @param {function} callback
   */
  [produceMessage] (body, ttl, callback) {
    const message = {
      uuid: uuid(),
      attempts: 1,
      data: body,
      time: new Date().getTime(),
      ttl: 0
    }

    this[sLogger].debug(`Message generated: [${message.uuid}]`)
    if (ttl) message.ttl = ttl
    this.client.lpush(this.keys.keyQueueName, JSON.stringify(message), (err) => {
      if (err) callback(err)
      else {
        if (this.stats) this.stats.incrementInputSlot()
        callback()
      }
    })
  }

  /**
   * @param {*} body
   * @param {function} callback
   */
  produce (body, callback) {
    this[produceMessage](body, null, callback)
  }

  /**
   * @param body
   * @param ttl
   * @param callback
   */
  produceWithTTL (body, ttl, callback) {
    this[produceMessage](body, ttl, callback)
  }

  /**
   * @param {function} callback
   */
  accureLock (callback) {
    const producerId = this.producerId
    const client = this.client
    const keyProducerLock = this.keys.keyProducerLock
    const keyProducerLockTmp = this.keys.keyProducerLockTmp

    const onUpdateLock = (err) => {
      if (err) callback(err)
      else callback(null, true)
    }

    const onLockTmp = (err, success) => {
      if (err) callback(err)
      else if (!success) callback(null, false)
      else client.set(keyProducerLock, producerId, 'EX', PRODUCER_LOCK_TTL, onUpdateLock)
    }

    const onGetLock = (err, id) => {
      if (err) callback(err)
      else if (id === producerId) client.set(keyProducerLock, producerId, 'EX', PRODUCER_LOCK_TTL, onUpdateLock)
      else if (id) callback(null, false)
      else client.set(keyProducerLockTmp, producerId, 'NX', 'EX', PRODUCER_LOCK_TTL, onLockTmp)
    }

    client.get(keyProducerLock, onGetLock)
  }
}

module.exports = Producer
