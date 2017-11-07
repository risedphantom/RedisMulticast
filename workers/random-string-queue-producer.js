'use strict'

const Producer = require('../core/producer')
const config = require('../config')
const loggerModule = require('../logger')
const uuid = require('uuid/v4')

const sLogger = Symbol('logger')

class RandomStringQueueProducer extends Producer {
  constructor (queueName, config) {
    super(queueName, config)
    this[sLogger] = loggerModule.getNewInstance(`random_string_queue_producer:${this.producerId}`, config.log)
  }

  run () {
    const message = uuid().replace(/-/g, '')

    this.produce(message, (err) => {
      if (err) {
        this[sLogger].error(`This should never happen. [${err}]`)
        return
      }

      this[sLogger].debug('Message published!')
    })

    setTimeout(this.run.bind(this), config.preferences.produce_interval)
  }
}

RandomStringQueueProducer.queueName = config.preferences.random_string_queue_name

module.exports = RandomStringQueueProducer
