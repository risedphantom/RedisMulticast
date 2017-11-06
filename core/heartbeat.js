'use strict'

const os = require('os')
const redisKeys = require('./redis-keys')
const redisClient = require('./redis-client')

/**
 * @param {object} consumer
 * @returns {object}
 */
function heartBeat (consumer) {
  const { queueName, consumerId, config } = consumer
  const { keyHeartBeat } = consumer.keys
  let client = null
  let halt = false

  function processHalt () {
    client.del(keyHeartBeat, (err) => {
      if (err) consumer.emit('error', err)

      client.end(true)
      client = null
      consumer.emit('heartbeat_halt')
    })
  }

  return {
    start () {
      halt = false
      client = redisClient.getNewInstance(config)
      this.beat()
    },

    stop () {
      halt = true
    },

    beat () {
      if (halt) {
        processHalt()
        return
      }

      const usage = {
        pid: process.pid,
        ram: {
          usage: process.memoryUsage(),
          free: os.freemem(),
          total: os.totalmem()
        },
        cpu: process.cpuUsage()
      }
      client.setex(keyHeartBeat, 10, JSON.stringify(usage), (err) => {
        if (err) consumer.emit('error', err)
        else {
          setTimeout(() => {
            this.beat()
          }, 1000)
        }
      })
    }
  }
}

/**
 * @param {Object} client
 * @param {string} queueName
 * @param {string} consumerId
 * @param {function} callback
 */
heartBeat.isOnline = function isOnline (client, queueName, consumerId, callback) {
  const keys = redisKeys.getKeys(queueName, consumerId)
  client.get(keys.keyHeartBeat, (err, res) => {
    if (err) callback(err)
    else callback(null, !!res)
  })
}

/**
 * @param {object} client
 * @param {function} callback
 */
heartBeat.getOnlineConsumers = function getOnlineConsumers (client, callback) {
  let heartBeatKeys = []
  const consumers = {}
  const rKeys = redisKeys.getKeys()
  const onConsumers = (err, values) => {
    if (err) callback(err)
    else {
      values.forEach((item, index) => {
        if (item) {
          const segments = redisKeys.getKeySegments(heartBeatKeys[index])
          if (!consumers.hasOwnProperty(segments.queueName)) (consumers[segments.queueName] = [])
          consumers[segments.queueName].push({
            consumerId: segments.consumerId,
            resources: JSON.parse(item)
          })
        }
      })
      callback(null, consumers)
    }
  }
  const onScanResults = (err, res0) => {
    if (err) callback(err)
    else {
      const [cur, keys] = res0
      if (keys && keys.length) {
        heartBeatKeys = keys
        client.mget(keys, onConsumers)
      } else callback(null, consumers)
    }
  }
  client.scan('0', 'match', rKeys.patternHeartBeat, 'count', 1000, onScanResults)
}

module.exports = heartBeat
