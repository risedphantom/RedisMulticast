'use strict'

const Consumer = require('../core/consumer')
const config = require('../config')
const loggerModule = require('../logger')

const sLogger = Symbol('logger')

class RandomStringQueueConsumer extends Consumer {
  constructor (config, options = {}) {
    super(config, options)
    this[sLogger] = loggerModule.getNewInstance(`random_string_queue_consumer:${this.consumerId}`, config.log)
  }

  /**
   * @param message
   * @param callback
   */
  consume (message, callback) {
    this[sLogger].trace(`Starting custom processing for message: "${JSON.stringify(message)}"`)

    // Stub processing
    if ((Math.random() * 5 | 0) === 0) callback(new Error('Processing message error!'))
    else callback()
  }
}

RandomStringQueueConsumer.queueName = config.preferences.random_string_queue_name

module.exports = RandomStringQueueConsumer
