'use strict'

const commander = require('commander')
const config = require('./config')
const Producer = require('./workers/random-string-queue-producer')
const Consumer = require('./workers/random-string-queue-consumer')
const Reader = require('./core/reader')
const uuid = require('uuid/v4')
const loggerModule = require('./logger')

commander
  .version('0.1.0')
  .option('-g, --get-errors', 'output failed messages and exit')
  .option('-p, --producer', 'run as producer')
  .option('-c, --consumer', 'run as consumer')

commander.parse(process.argv)

const currentInstance = uuid()
const logger = loggerModule.getNewInstance(`main_thread:${currentInstance}`, config.log)
// TODO: Register this instance somewhere with current UUID to be able to collect logs
logger.info('-= REDIS MULTICAST START =-')

if (commander.getErrors) {
  logger.info('FAILED MESSAGES:')
  const reader = new Reader(config.preferences.random_string_queue_name, config)
  reader.readDeadQueue((err, messages) => {
    if (err) logger.error(`Error occurred while reading dead message queue [${err}]`)
    else for (let message of messages) logger.info(message)

    process.exit(0)
  })
}

if (commander.producer) {
  logger.info('-= RUNNING AS PRODUCER =-')
  const producer = new Producer(config.preferences.random_string_queue_name, config)
  producer.run()
}

if (commander.consumer) {
  logger.info('-= RUNNING AS CONSUMER =-')
  const consumer = new Consumer(config, {messageRetryThreshold: 1})
  consumer.run()
}
