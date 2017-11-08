'use strict'

const Consumer = require('../core/consumer')
const config = require('../config')
const loggerModule = require('../logger')

const sLogger = Symbol('logger')

class RandomStringQueueConsumer extends Consumer {
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
    super(config, options)
    this[sLogger] = loggerModule.getNewInstance(`random_string_queue_consumer:${this.consumerId}`, config.log)

    this[sLogger].debug(`Consumer [${this.consumerId}] created.`)
  }

  /**
   * @param message
   * @param callback
   */
  consume (message, callback) {
    this[sLogger].debug(`Starting custom processing for message: "${JSON.stringify(message)}"`)

    // Stub processing
    if ((Math.random() * 20 | 0) === 0) callback(new Error('5 percent chance of failure'))
    else callback()
  }

  /**
   * @param {string} producerId
   * @param callback
   */
  canRun (producerId, callback) {
    const logger = this[sLogger]
    const key = this.keys.keyProducerLock

    this.getSuspendFlag(key, (err, id) => {
      if (err) logger.error(`Error while getting producer lock. [${err}]`)
      else callback(null, (id && id !== producerId))
    })
  }
}

RandomStringQueueConsumer.queueName = config.preferences.random_string_queue_name

module.exports = RandomStringQueueConsumer
