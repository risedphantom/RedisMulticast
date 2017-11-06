'use strict'

const config = require('./config')
const Producer = require('./core/producer')
const Consumer = require('./workers/random-string-queue-consumer')
const uuid = require('uuid/v4')
const loggerModule = require('./logger')

const currentInstance = uuid()
// TODO: Register this instance somewhere with current UUID to be able to collect logs
const producer = new Producer(config.preferences.random_string_queue_name, config)
const consumer = new Consumer(config)
const logger = loggerModule.getNewInstance(`main_thread:${currentInstance}`, config.log)

function onProduce (err) {
  if (err) {
    logger.error(`This should never happen. [${err}]`)
    return
  }

  logger.trace('Message produced!')
}

setInterval(() => {
  const message = uuid().replace(/-/g, '')

  producer.produce(message, (err) => {
    if (err) onProduce(err)
    else onProduce()
  })
}, config.preferences.produce_interval)

// consumer.run()
