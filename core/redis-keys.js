'use strict'

module.exports = {

  /**
   * @param {string} [queueName]
   * @param {string} [consumerId]
   * @param {string} [producerId]
   * @returns {object}
   */
  getKeys (queueName, consumerId, producerId) {
    if (queueName && queueName.indexOf('queue:') === 10) {
      queueName = queueName.split(':')[2]
    }
    const keys = {}
    keys.patternQueueNameDead = 'dead:*'
    keys.patternQueueNameProcessing = 'processing:*'
    keys.patternQueueName = 'queue:*'
    keys.patternHeartBeat = 'heartbeat:*'
    keys.patternGC = 'gc:*'
    keys.patternRate = 'rate:*'
    keys.patternRateProcessing = 'rate:processing:*'
    keys.patternRateAcknowledged = 'rate:acknowledged:*'
    keys.patternRateUnacknowledged = 'rate:unacknowledged:*'
    keys.patternRateInput = 'rate:input:*'
    keys.keyStatsFrontendLock = 'stats:frontend:lock'
    if (queueName) {
      keys.patternQueueNameProcessing = `processing:${queueName}:*`
      keys.keyQueueName = `queue:${queueName}`
      keys.keyQueueNameDead = `dead:${queueName}`
      keys.keyGCLock = `gc:${queueName}:lock`
      keys.keyGCLockTmp = `${keys.keyGCLock}:tmp`
      if (consumerId) {
        keys.keyQueueNameProcessing = `processing:${queueName}:${consumerId}`
        keys.keyHeartBeat = `heartbeat:${queueName}:${consumerId}`
        keys.keyRateProcessing = `rate:processing:${queueName}:${consumerId}`
        keys.keyRateAcknowledged = `rate:acknowledged:${queueName}:${consumerId}`
        keys.keyRateUnacknowledged = `rate:unacknowledged:${queueName}:${consumerId}`
      }
      if (producerId) {
        keys.keyRateInput = `rate:input:${queueName}:${producerId}`
      }
    }
    const ns = 'redismq'
    for (const k in keys) keys[k] = `${ns}:${keys[k]}`
    return keys
  },

  /**
   * @param {string} key
   * @returns {object}
   */
  getKeySegments (key) {
    if (key.indexOf('processing:') === 8) {
      const [, , queueName, consumerId] = key.split(':')
      return {
        queueName,
        consumerId
      }
    }
    if (key.indexOf('rate:') === 8) {
      const [, , type, queueName, id] = key.split(':')
      return {
        type,
        queueName,
        id
      }
    }
    if (key.indexOf('heartbeat:') === 8) {
      const [, , queueName, consumerId] = key.split(':')
      return {
        queueName,
        consumerId
      }
    }
    if (key.indexOf('dead:') === 8 || key.indexOf('queue:') === 8) {
      const [, , queueName] = key.split(':')
      return {
        queueName
      }
    }
    return {}
  }
}
