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

const producer = new Producer(config.preferences.random_string_queue_name, config)
producer.run()

const consumer = new Consumer(config, {messageRetryThreshold: 1})

setTimeout(() => {
  consumer.canRun(producer.producerId, (err, can) => {
    if (err) logger.error(`Error occurred while trying to run consumer [${err}]`)
    if (can) {
      consumer.run()

      const tryToStop = () => {
        consumer.canRun(producer.producerId, (err, can) => {
          if (err) logger.error(`Error occurred while trying to stop consumer [${err}]`)
          if (!can) consumer.stop()
          else setTimeout(tryToStop, config.preferences.try_to_run_interval)
        })
      }

      tryToStop()
    }
  })
}, 1000)
