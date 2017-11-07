'use strict'

module.exports = {
  redis: {
    host: 'anpanov.ru',
    port: 6666
  },
  log: {
    enabled: 1,
    options: {
      level: 'debug'
    }
  },
  monitor: {
    enabled: true,
    host: '127.0.0.1',
    port: 3000
  },

  preferences: {
    random_string_queue_name: 'random_string_queue',
    produce_interval: 500
  }
}
