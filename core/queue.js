'use strict'

const redisKeys = require('./redis-keys')

module.exports = {

  /**
   * @param {RedisClient} client
   * @param {function} callback
   */
  getQueues (client, callback) {
    const keys = redisKeys.getKeys()
    client.scan('0', 'match', keys.patternQueueName, 'count', 1000, (err, res) => {
      if (err) callback(err)
      else {
        const [queues] = res
        callback(null, queues)
      }
    })
  },

  /**
   * @param {RedisClient} client
   * @param {function} callback
   */
  getDeadLetterQueues (client, callback) {
    const keys = redisKeys.getKeys()
    client.scan('0', 'match', keys.patternQueueNameDead, 'count', 1000, (err, res) => {
      if (err) callback(err)
      else {
        const [queues] = res
        callback(null, queues)
      }
    })
  },

  /**
   * @param {RedisClient} client
   * @param {Array} queues
   * @param {function} callback
   */
  calculateQueueSize (client, queues, callback) {
    const queuesList = []
    if (queues && queues.length) {
      const multi = client.multi()
      for (const queueName of queues) multi.llen(queueName)
      multi.exec((err, res) => {
        if (err) callback(err)
        else {
          res.forEach((size, index) => {
            const segments = redisKeys.getKeySegments(queues[index])
            queuesList.push({
              name: segments.queueName,
              size
            })
          })
          callback(null, queuesList)
        }
      })
    } else callback(null, queuesList)
  }
}
