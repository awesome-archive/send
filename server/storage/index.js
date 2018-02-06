const config = require('../config');
const mozlog = require('../log');
const createRedisClient = require('./redis');

const log = mozlog('send.storage');

class DB {
  constructor(config) {
    const Storage = config.s3_bucket ? require('./s3') : require('./fs');
    this.expireSeconds = config.expire_seconds;
    this.storage = new Storage(config, log);
    this.redis = createRedisClient(config);
    this.redis.on('error', err => {
      log.error('Redis:', err);
    });
  }

  async exists(id) {
    const result = await this.redis.existsAsync(id);
    return result === 1;
  }

  async ttl(id) {
    const result = await this.redis.ttlAsync(id);
    return result * 1000;
  }

  length(id) {
    return this.storage.length(id);
  }

  get(id) {
    return this.storage.getStream(id);
  }

  async set(id, file, meta) {
    await this.storage.set(id, file);
    this.redis.hmset(id, meta);
    this.redis.expire(id, this.expireSeconds);
  }

  setField(id, key, value) {
    this.redis.hset(id, key, value);
  }

  async del(id, ownerToken) {
    const owner = await this.redis.hgetAsync(id, 'owner');
    if (owner !== ownerToken) {
      throw new Error('unauthorized');
    }
    return this.forceDelete(id);
  }

  forceDelete(id) {
    this.redis.del(id);
    return this.storage.del(id);
  }

  async ping() {
    await this.redis.pingAsync();
    await this.storage.ping();
  }

  async metadata(id) {
    const result = await this.redis.hgetallAsync(id);
    //TODO parse string values into proper types
    return result;
  }

  quit() {
    this.redis.quit();
  }
}

module.exports = new DB(config);
