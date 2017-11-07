'use strict'

const config = require('./config')
const Producer = require('./workers/random-string-queue-producer')
const Consumer = require('./workers/random-string-queue-consumer')
const uuid = require('uuid/v4')
const loggerModule = require('./logger')

const currentInstance = uuid()
const logger = loggerModule.getNewInstance(`main_thread:${currentInstance}`, config.log)
// TODO: Register this instance somewhere with current UUID to be able to collect logs
logger.debug('-= REDIS MULTICAST START =-')

const producer = new Producer(config.preferences.random_string_queue_name, config)
const consumer = new Consumer(config, {messageRetryThreshold: 1})

producer.run()
//consumer.run()
