'use strict'

const Producer = require('../core/producer')
const config = require('../config')
const loggerModule = require('../logger')
const uuid = require('uuid/v4')

const sLogger = Symbol('logger')

class RandomStringQueueProducer extends Producer {
  /**
   * @param {string} queueName
   * @param {object} config
   */
  constructor (queueName, config) {
    super(queueName, config)
    this[sLogger] = loggerModule.getNewInstance(`random_string_queue_producer:${this.producerId}`, config.log)

    this[sLogger].debug(`Producer [${this.producerId}] created.`)
  }

  run () {
    const that = this

    const tryLock = () => {
      that.accureLock((err, success) => {
        if (err) that[sLogger].error(`Error while accuring lock. [${err}]`)
        else if (success) {
          clearInterval(interval)

          const message = uuid().replace(/-/g, '')
          that.produce(message, (err) => {
            if (err) {
              that[sLogger].error(`This should never happen. [${err}]`)
              return
            }

            that[sLogger].debug('Message published!')
          })

          setTimeout(tryLock.bind(that), config.preferences.produce_interval)
        }
      })
    }

    let interval = setInterval(tryLock, config.preferences.accure_lock_interval)
  }
}

RandomStringQueueProducer.queueName = config.preferences.random_string_queue_name

module.exports = RandomStringQueueProducer
