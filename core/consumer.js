'use strict'

const EventEmitter = require('events')
const uuid = require('uuid/v4')
const redisKeys = require('./redis-keys')
const redisClient = require('./redis-client')
const heartBeat = require('./heartbeat')
const statsFactory = require('./stats')
const garbageCollector = require('./gc')
const logger = require('../logger')

const CONSUMER_STATUS_GOING_DOWN = 0
const CONSUMER_STATUS_DOWN = 1
const CONSUMER_STATUS_GOING_UP = 2
const CONSUMER_STATUS_UP = 3
const CONSUMER_STATUS_CONSUMING = 4

const sRegisterEvents = Symbol('registerEvents')
const sGetEventsHandlers = Symbol('getEventsHandlers')
const sGetNextMessage = Symbol('getNextMessage')
const sConsumeMessage = Symbol('consumeMessage')
const sProcessMessageFailure = Symbol('processMessageFailure')
const sHeartBeat = Symbol('heartBeat')
const sGarbageCollector = Symbol('garbageCollector')
const sStats = Symbol('stats')
const sLogger = Symbol('logger')
const sRedisClient = Symbol('redisClient')

class Consumer extends EventEmitter {
  /**
   * @param {object} config
   * @param {object} config.redis
   * @param {string} config.redis.host
   * @param {number} config.redis.port
   * @param {object} config.log
   * @param {(boolean|number)} config.log.enabled
   * @param {object} config.log.options
   * @param {object} config.monitor
   * @param {(boolean|number)} config.monitor.enabled
   * @param {string} config.monitor.host
   * @param {number} config.monitor.port
   * @param {object} options
   * @param {number} options.messageConsumeTimeout
   * @param {number} options.messageTTL
   * @param {number} options.messageRetryThreshold
   */
  constructor (config, options = {}) {
    super()

    if (!this.constructor.hasOwnProperty('queueName')) throw new Error('Undefined queue name!')

    EventEmitter.call(this)
    this[sRegisterEvents]()

    this.consumerId = uuid()
    this.queueName = this.constructor.queueName
    this.config = config
    this.options = options
    this.messageConsumeTimeout = options.hasOwnProperty('messageConsumeTimeout') ? Number(options.messageConsumeTimeout) : 0
    this.messageTTL = options.hasOwnProperty('messageTTL') ? Number(options.messageTTL) : 0
    this.keys = redisKeys.getKeys(this.queueName, this.consumerId)
    this.isTest = process.env.NODE_ENV === 'test'
    this.status = CONSUMER_STATUS_DOWN

    this[sLogger] = logger.getNewInstance(`${this.queueName}:${this.consumerId}`, config.log)

    this[sHeartBeat] = heartBeat(this)

    this[sGarbageCollector] = garbageCollector(this, this[sLogger])

    const monitorEnabled = !!(config.monitor && config.monitor.enabled)
    if (monitorEnabled) this[sStats] = statsFactory(this, config)
  }

  /**
   * @returns {object}
   */
  [sGetEventsHandlers] () {
    const consumer = this
    return {
      halt () {
        consumer.status = CONSUMER_STATUS_DOWN
        consumer.emit('halt')
      },

      /**
       * If an error occurred, the whole consumer should go down,
       * A consumer can not exit without heartbeat/gc and vice-versa
       *
       * @param err
       */
      onError (err) {
        if (err.name === 'AbortError' || consumer.status === CONSUMER_STATUS_GOING_DOWN) return

        consumer[sLogger].error(err)
        process.exit(1)
      },

      onHeartBeatHalt () {
        if (consumer[sStats]) consumer[sStats].stop()
        else this.halt()
      },

      onConsumerHalt () {
        consumer.status = CONSUMER_STATUS_GOING_DOWN
        consumer[sRedisClient].end(true)
        delete consumer[sRedisClient]
        consumer[sGarbageCollector].stop()
      },

      onGCHalt () {
        consumer[sHeartBeat].stop()
      },

      /**
       * @param message
       */
      onConsumeTimeout (message) {
        consumer[sProcessMessageFailure](message, new Error(`Consumer timed out after [${consumer.messageConsumeTimeout}]`))
      },

      /**
       * @param message
       */
      onMessageExpired (message) {
        consumer[sLogger].trace(`Message [${message.uuid}] has expired`)
        consumer[sGarbageCollector].collectExpiredMessage(message, consumer.keys.keyQueueNameProcessing, () => {
          if (consumer[sStats]) consumer[sStats].incrementAcknowledgedSlot()
          consumer[sLogger].trace(`Message [${message.uuid}] successfully processed`)
          consumer.emit('next')
        })
      },

      /**
       * @param message
       */
      onMessage (message) {
        if (consumer.status !== CONSUMER_STATUS_UP) return

        if (consumer[sGarbageCollector].checkMessageExpiration(message)) consumer.emit('message_expired', message)
        else consumer[sConsumeMessage](message)
      },

      onNext () {
        if (!consumer.isRunning()) return

        consumer.status = CONSUMER_STATUS_UP
        consumer[sGetNextMessage]()
      }
    }
  }

  [sRegisterEvents] () {
    const handlers = this[sGetEventsHandlers]()

    /**
     * Events
     */
    this
      .on('next', handlers.onNext)
      .on('message', handlers.onMessage)
      .on('message_expired', handlers.onMessageExpired)
      .on('consume_timeout', handlers.onConsumeTimeout)
      .on('consumer_halt', handlers.onConsumerHalt)
      .on('gc_halt', handlers.onGCHalt)
      .on('heartbeat_halt', handlers.onHeartBeatHalt)
      .on('stats_halt', handlers.halt)
      .on('error', handlers.onError)
  }

  /**
   * @param {object} message
   * @param {object} error
   */
  [sProcessMessageFailure] (message, error) {
    this[sLogger].error(`Consume failure! Message: [${message.id}]. Reason: [${error}]`)
    if (this[sStats]) this[sStats].incrementUnacknowledgedSlot()
    this[sGarbageCollector].collectMessage(message, this.keys.keyQueueNameProcessing, error, (err) => {
      if (err) this.emit('error', err)
      else this.emit('next')
    })
  }

  [sGetNextMessage] () {
    this[sLogger].trace('Waiting for new messages...')
    this[sRedisClient].brpoplpush(this.keys.keyQueueName, this.keys.keyQueueNameProcessing, 0, (err, payload) => {
      if (err) this.emit('error', err)
      else {
        this[sLogger].trace('Got new message...')
        if (this[sStats]) this[sStats].incrementProcessingSlot()
        const message = JSON.parse(payload)
        this.emit('message', message)
      }
    })
  }

  /**
   * @param {object} message
   */
  [sConsumeMessage] (message) {
    this.status = CONSUMER_STATUS_CONSUMING
    let isTimeout = false
    let timer = null
    this[sLogger].trace(`Processing message [${message.uuid}]...`)

    try {
      if (this.messageConsumeTimeout) {
        timer = setTimeout(() => {
          isTimeout = true
          timer = null
          this.emit('consume_timeout', message)
        }, this.messageConsumeTimeout)
      }
      const onDeleted = (err) => {
        if (err) this.emit('error', err)
        else {
          if (this[sStats]) this[sStats].incrementAcknowledgedSlot()
          this[sLogger].trace(`Message [${message.uuid}] successfully processed`)
          this.emit('next')
          if (this.isTest) this.emit('message_consumed', JSON.stringify(message))
        }
      }
      const onConsumed = (err) => {
        if (!isTimeout) {
          if (timer) clearTimeout(timer)
          if (err) throw err
          // when a consumer is stopped, redis client instance is destroyed
          if (this[sRedisClient]) {
            this[sRedisClient].del(this.keys.keyQueueNameProcessing, onDeleted)
          } else if (this.isRunning()) {
            throw new Error('Redis client instance has gone!')
          }
        }
      }
      this.consume(message.data, onConsumed)
    } catch (error) {
      this[sProcessMessageFailure](message, error)
    }
  }

  run () {
    if (this.status === CONSUMER_STATUS_DOWN) {
      this.status = CONSUMER_STATUS_GOING_UP

      this[sHeartBeat].start()
      this[sGarbageCollector].start()
      if (this[sStats]) this[sStats].start()

      /**
       * Wait for messages
       */
      this[sRedisClient] = redisClient.getNewInstance(this.config)
      this.emit('next')
    }
  }

  stop () {
    if (this.isRunning()) this.emit('consumer_halt')
  }

  /**
   * @returns {boolean}
   */
  isRunning () {
    return ([CONSUMER_STATUS_GOING_DOWN, CONSUMER_STATUS_DOWN].indexOf(this.status) === -1)
  }

  /**
   * @param {*} message
   * @param {function} callback
   */
  static consume (message, callback) {
    /* eslint class-methods-use-this: 0 */
    throw new Error('Consume method should be extended')
  }
}

module.exports = Consumer
