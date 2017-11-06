'use strict'

const redisKeys = require('./redis-keys')
const redisClient = require('./redis-client')
const heartBeat = require('./heartbeat')

const GC_ACQUIRE_LOCK_RETRY_INTERVAL = 2000 // in ms (2 seconds)
const GC_SCAN_QUEUES_INTERVAL = 2000 // in ms (2 second)
const GC_MESSAGE_RETRY_THRESHOLD = 3 // 3 times

/**
 * @param {object} consumer
 * @param {object} logger
 * @returns {object}
 */
function garbageCollector (consumer, logger) {
  const { consumerId, queueName, config } = consumer
  const { patternQueueNameProcessing, keyQueueName, keyQueueNameDead, keyGCLock, keyGCLockTmp } = consumer.keys
  const messageRetryThreshold = consumer.options.messageRetryThreshold || GC_MESSAGE_RETRY_THRESHOLD

  /**
   * @type {(object|null)}
   */
  let client = null

  /**
   * @type {boolean}
   */
  let halt = false

  /**
   * @type {boolean}
   */
  let lockAcquired = false

  /**
   * @param {string} message
   */
  function debug (message) {
    logger.debug({ gc: true }, message)
  }

  /**
   * @param {function} callback
   */
  function lockNX (callback) {
    const onGCLock = (err) => {
      if (err) consumer.emit('error', err)
      else {
        debug('Lock acquired!')
        lockAcquired = true
        callback()
      }
    }
    const onGCTmpLock = (err, success) => {
      if (err) consumer.emit('error', err)
      else if (success) {
        client.set(keyGCLock, consumerId, onGCLock)
      } else acquireLockRetry(callback)
    }
    client.set(keyGCLockTmp, consumerId, 'NX', 'EX', 60, onGCTmpLock)
  }

  /**
   * @param {function} callback
   */
  function acquireLock (callback) {
    if (!lockAcquired) {
      debug('Trying to acquire a lock...')
      const onConsumerOnline = (err, online) => {
        if (err) consumer.emit('error', err)
        else if (online) acquireLockRetry(callback)
        else lockNX(callback)
      }
      const onGCLock = (err, id) => {
        if (err) consumer.emit('error', err)
        else if (id) {
          heartBeat.isOnline(client, queueName, id, onConsumerOnline)
        } else lockNX(callback)
      }
      client.get(keyGCLock, onGCLock)
    } else callback()
  }

  /**
   * @param {function} callback
   */
  function releaseLock (callback) {
    if (lockAcquired) {
      debug('Releasing lock...')
      const success = () => {
        lockAcquired = false
        debug('Lock released!')
        callback()
      }
      const onTmpLockKeyDeleted = (err) => {
        if (err) consumer.emit('error', err)
        else success()
      }
      const onTmpLock = (err, key) => {
        if (err) consumer.emit('error', err)
        else if (key === consumerId) client.del(keyGCLockTmp, onTmpLockKeyDeleted)
        else success()
      }
      const onLockKeyDeleted = (err) => {
        if (err) consumer.emit('error', err)
        else client.get(keyGCLockTmp, onTmpLock)
      }
      client.del(keyGCLock, onLockKeyDeleted)
    } else callback()
  }

  /**
   * @param {function} callback
   */
  function acquireLockRetry (callback) {
    if (!halt) {
      setTimeout(() => {
        acquireLock(callback)
      }, GC_ACQUIRE_LOCK_RETRY_INTERVAL)
    } else haltProcess()
  }

  /**
   * @param {object} args
   * @param {Array} args.keys
   * @param {function} args.callback
   * @param {number} args.index
   * @param {number} args.length
   */
  function checkMessages (args) {
    if (!args.hasOwnProperty('index')) args.index = 0
    if (!args.hasOwnProperty('length')) args.length = args.keys.length
    const { length, keys, callback } = args
    let index = args.index
    if (index < length) {
      const pqName = keys[index]
      debug(`Checking processing queue [${pqName}]... `)
      const segments = redisKeys.getKeySegments(pqName)
      debug(`Is consumer ID [${segments.consumerId}] alive?`)
      index += 1
      const onMessageCollected = (err) => {
        if (err) consumer.emit('error', err)
        else checkMessages({ index, length, keys, callback })
      }
      const onRange = (err, range) => {
        if (err) consumer.emit('error', err)
        else {
          const message = JSON.parse(range[0])
          debug(`Collecting message [${message.uuid}]...`)
          if (checkMessageExpiration(message)) collectExpiredMessage(message, pqName, onMessageCollected)
          else collectMessage(message, pqName, null, onMessageCollected)
        }
      }
      const onConsumerOnline = (err, online) => {
        if (err) consumer.emit('error', err)
        else if (online) {
          debug(`Consumer ID [${segments.consumerId}] is alive!`)
          checkMessages({ index, length, keys, callback })
        } else {
          debug(`Consumer ID [${segments.consumerId}] seems to be dead. Fetching queue message...`)
          client.lrange(pqName, 0, 0, onRange)
        }
      }
      heartBeat.isOnline(client, segments.queueName, segments.consumerId, onConsumerOnline)
    } else callback()
  }

  function checkProcessingQueuesRetry () {
    if (!halt) {
      debug(`Waiting for ${GC_SCAN_QUEUES_INTERVAL} before a new iteration...`)
      setTimeout(() => {
        debug('Time is up...')
        checkProcessingQueues()
      }, GC_SCAN_QUEUES_INTERVAL)
    } else haltProcess()
  }

  /**
   * @param {string} cursor
   */
  function checkProcessingQueues (cursor = '0') {
    acquireLock(() => {
      debug(`Scanning for queues having pattern [${patternQueueNameProcessing}]...`)
      client.scan(cursor, 'match', patternQueueNameProcessing, 'count', 100, (err, res) => {
        if (err) consumer.emit('error', err)
        else {
          const [cur, keys] = res
          if (keys && keys.length) {
            debug(`Found [${keys.length}] keys`)
            const callback = () => {
              if (cur !== '0') {
                debug(`Processing next items from cursor [${cur}]...`)
                checkProcessingQueues(cur)
              } else {
                debug('No more items')
                checkProcessingQueuesRetry()
              }
            }
            checkMessages({ keys, callback })
          } else {
            debug('No queues found')
            checkProcessingQueuesRetry()
          }
        }
      })
    })
  }

  /**
   * Move message to dead-letter queue when max attempts threshold is reached
   * otherwise requeue it again
   *
   * @param {object} message
   * @param {string} processingQueue
   * @param {object} error
   * @param {function} callback
   */
  function collectMessage (message, processingQueue, error, callback) {
    let destQueueName = ''
    let logInfo = ''
    message.attempts += 1
    message.error = error || {}
    if (message.attempts > messageRetryThreshold) {
      logInfo = `Moving message (ID [${message.uuid}], attempts [${message.attempts}]) to dead-letter queue...`
      destQueueName = keyQueueNameDead
    } else {
      logInfo = `Re-queuing message (ID [${message.uuid}], attempts [${message.attempts}])...`
      destQueueName = keyQueueName
    }
    const messageString = JSON.stringify(message)
    const multi = client.multi()
    client.lpush(destQueueName, messageString)
    client.del(processingQueue)
    debug(logInfo)
    multi.exec((err) => {
      if (err) callback(err)
      else {
        if (consumer.isTest) {
          if (destQueueName === keyQueueNameDead) consumer.emit('message_dead_queue', messageString)
          else consumer.emit('message_requeued', messageString)
        }
        callback()
      }
    })
  }

  /**
   * @param {object} message
   * @param {string} processingQueue
   * @param {function} callback
   */
  function collectExpiredMessage (message, processingQueue, callback) {
    debug(`Processing expired message [${message.uuid}]...`)
    client.del(processingQueue, (err) => {
      if (err) consumer.emit('error', err)
      else {
        callback()
        if (consumer.isTest) consumer.emit('message_destroyed', JSON.stringify(message))
      }
    })
  }

  function haltProcess () {
    releaseLock(() => {
      client.end(true)
      client = null
      halt = false
      consumer.emit('gc_halt')
    })
  }

  /**
   * @param {object} message
   * @returns {boolean}
   */
  function checkMessageExpiration (message) {
    let expired = false
    if (message.ttl || consumer.messageTTL) {
      const curTime = new Date().getTime()
      expired = (message.ttl && ((message.time + message.ttl) - curTime) < 0) ||
        (consumer.messageTTL && ((message.time + consumer.messageTTL) - curTime) < 0)
    }
    return expired
  }

  return {
    start () {
      if (!halt) {
        client = redisClient.getNewInstance(config)
        checkProcessingQueues()
      }
    },

    stop () {
      halt = true
    },

    collectMessage,

    collectExpiredMessage,

    checkMessageExpiration
  }
}

module.exports = garbageCollector
