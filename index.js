var EE = require('events').EventEmitter
var join = require('path').join
var qs = require('querystring')
var aws = require('aws-sdk')
var through = require('through')
var PassThrough = require('stream').PassThrough
var Readable = require('stream').Readable

module.exports = S3Backend

function S3Backend(options) {
  if (!(this instanceof S3Backend)) {
    return new S3Backend(options)
  }

  EE.call(this)

  this.options = options || {}
  this.s3 = new aws.S3(options.s3)
}

S3Backend.prototype = Object.create(EE.prototype)
S3Backend.prototype.constructor = S3Backend

S3Backend.prototype.getUser = getter('users', '~/users/')
S3Backend.prototype.setUser = setter('users', '~/users/', 'setUser', 'getUser')
S3Backend.prototype.removeUser = remover('users', '~/users/', 'removeUser', 'getUser')
S3Backend.prototype.createUserStream = streamer('users', '~/users/', [])
S3Backend.prototype._setMeta = setter('meta', '', 'setMeta', 'getMeta')
S3Backend.prototype.getMeta = getter('meta', '')
S3Backend.prototype.removeMeta = remover('meta', '', 'removeMeta', 'getMeta')
S3Backend.prototype.createMetaStream = streamer('meta', '', [])
S3Backend.prototype.get = getter('store', '~/store/')
S3Backend.prototype.set = setter('store', '~/store/', 'set', 'get')
S3Backend.prototype.remove = remover('store', '~/store/', 'remove', 'get')
S3Backend.prototype.createStream = streamer('store', '~/store/', [])

S3Backend.prototype.setMeta = function setMeta(name, meta, done) {
  var escaped = escape(name)
  var baseUrl = this.options.baseUrl || ''
  if(baseUrl && baseUrl[baseUrl.length - 1] === '/') {
    baseUrl = baseUrl.slice(0, -1)
  }

  if (meta.versions) {
    Object.keys(meta.versions).forEach(function(version) {
      var url = baseUrl + join('/', escaped, '-', escaped + '-' + version + '.tgz')
      meta.versions[version].dist = meta.versions[version].dist || {}
      meta.versions[version].dist.tarball = url

      if(!meta.versions[version]._id) {
        meta.versions[version]._id = name + '@' + version
      }
    })
  }

  this._setMeta(name, meta, done)
}

S3Backend.prototype.getTarball = function getTarball(_name, version) {
  var stream = new PassThrough()
  var name = escape(_name)
  var params = {}
  if (this.options.tarballs) {
    params.Bucket = this.options.tarballs.Bucket
    name = (this.options.tarballs.prefix || '') + name
  }

  params.Key = name + '/-/' + name + '-' + version + '.tgz'

  this.s3.getObject(params, function write(err, data) {
    console.log(err, params)
    if (err) return stream.emit('error', err)
    stream.end(data.Body)
  })

  return stream
}

S3Backend.prototype.setTarball = function setTarball(_name, version) {
  var stream = new PassThrough()
  var name = escape(_name)
  var params = {}

  if (this.options.tarballs) {
    params.Bucket = this.options.tarballs.Bucket
    name = (this.options.tarballs.prefix || '') + name
  }

  params.Key = name + '/-/' + name + '-' + version + '.tgz'
  params.ContentType = 'application/x-compressed'
  params.Body = stream

  this.s3.upload(params, function uploaded(err) {
    if (err) return stream.emit('error', err)
  })

  return stream
}

S3Backend.prototype.removeTarball = function removeTarball(_name, version, _done) {
  var params = {}
  var name = escape(_name)
  var done = _done || new Function

  if (this.options.tarballs) {
    params.Bucket = this.options.tarballs.Bucket
    name = (this.options.tarballs.prefix || '') + name
  }

  params.Key = name + '/-/' + name + '-' + version + '.tgz'

  this.s3.deleteObject(params, function removed(err, data) {
    if (err) return done(err, null)
    done(null)
  })
}

S3Backend.prototype.getFileList = function getFileList(bucket, prefix, ignores, done) {
  var params = {Bucket: bucket, Delimiter: '/'}
  if ('prefix') params.Prefix = prefix
  var backend = this
  var items = []

  getSome()

  function getSome(marker) {
    if(marker) params.marker = marker
    backend.s3.listObjects(params, function gotSome(err, data) {
      if (err) return done(err)
      items = items.concat(data.Contents.map(addName).filter(notIgnored))
      if (data.NextMarker) return getSome(data.NextMarker)
      done(null, items)
    })
  }

  function addName(item) {
    var name = item && unescape(item.Key.slice(prefix.length))
    if(!name) return null
    return {name: name, params: {Bucket: bucket, Key: item.Key}}
  }

  function notIgnored(name) {
    if (!name) return false
    for (var i = ignores.length - 1; i >= 0; --i) {
      if (ignores.test(name)) return false
    }

    return true
  }
}

function getter(type, prefix) {
  return function get(_name, _done) {
    var done = _done || new Function
    var name = escape(_name)
    var params = {}
    if (this.options[type]) {
      params.Bucket = this.options[type].Bucket
      name = (this.options[type].prefix || prefix) + name
    } else {
      name = prefix + name
    }

    params.Key = name

    this.s3.getObject(params, function parse(err, data) {
      if (err) return done(err.statusCode === 404 ? null : err, null)

      try {
        data = JSON.parse(data.Body.toString())
      } catch (err) {
        return done(err, null)
      }

      done(null, data)
    })
  }
}

function setter(type, prefix, eventName, getter) {
  return function set(_name, data, _done) {
    var backend = this
    var params = {}
    var name = escape(_name)
    var done = _done || new Function

    if (!data && data !== '') return done(new Error('Must provide data to store'))

    try {
      params.Body = JSON.stringify(data, null, 2)
    } catch (err) {
      return done(err)
    }

    if (this.options[type]) {
      params.Bucket = this.options[type].Bucket
      name = (this.options[type].prefix || prefix) + name
    } else {
      name = prefix + name
    }

    params.Key = name
    params.ContentType = 'application/json'

    this[getter](_name, gotPrevious)

    function gotPrevious(err, previous) {
      if (err) return done(err, null)
      backend.s3.upload(params, function uploaded(err) {
        if (err) return done(err, null)
        done(null, data, previous)
        backend.emit(eventName, _name, data, previous)
      })
    }
  }
}

function remover(type, prefix, eventName, getter) {
  return function remove(_name, _done) {
    var backend = this
    var params = {}
    var name = escape(_name)
    var done = _done || new Function

    if (this.options[type]) {
      params.Bucket = this.options[type].Bucket
      name = (this.options[type].prefix || prefix) + name
    } else {
      name = prefix + name
    }

    params.Key = name

    this[getter](_name, gotPrevious)

    function gotPrevious(err, previous) {
      if (err) return done(err, null)
      backend.s3.deleteObject(params, function removed(err, data) {
        if (err) return done(err, null)
        done(null, previous)
        backend.emit(eventName, _name, previous)
      })
    }
  }
}

function streamer(type, prefix, ignores) {
  return function stream(options) {
    var stream = new ContentStream(this.s3, options)
    var bucket = this.options.s3.params.Bucket
    if (this.options[type] && this.options[type].Bucket) {
      bucket = this.options[type].Bucket
    }

    this.getFileList(bucket, prefix, ignores, function gotFiles(err, items) {
      if (err) return stream.emit('error', err)
      stream.init(items)
    })

    return stream
  }
}

function escape(name) {
  return name.replace('/', '%2f')
}

function unescape(name) {
  return name.replace('%2f', '/')
}

function ContentStream(s3, options) {
  if (!(this instanceof ContentStream)) {
    return new ContentStream(s3, options)
  }

  Readable.call(this, {objectMode: true})
  this.options = options || {}
  this.reading = false
  this.items = null
  this.s3 = s3
}

ContentStream.prototype = Object.create(Readable.prototype)
ContentStream.prototype.constructor = ContentStream

ContentStream.prototype.init = function init(items) {
  var opts = this.options
  this.items = items.filter(byRange)
  if (opts.reverse) this.items.reverse()
  if (opts.limit >= 0) this.items = items.slice(opts.limit)
  if (this.reading) this.readOne()

  function byRange(item) {
    if (opts.gt && !(item.name > opts.gt)) return false
    if (opts.start && !(item.name >= opts.start)) return false
    if (opts.gte && !(item.name >= opts.gt)) return false
    if (opts.lt && !(item.name < opts.gt)) return false
    if (opts.lte && !(item.name <= opts.gt)) return false
    if (opts.end && !(item.name <= opts.end)) return false
    return true
  }
}

ContentStream.prototype._read = function read() {
  if (this.items) return this.readOne()
  this.reading = true
}

ContentStream.prototype.readOne = function readOne() {
  if (!this.items || !this.items.length) return this.push(null)
  var item = this.items.shift()
  var stream = this

  if (!stream.options.values && typeof stream.options.values !== 'undefined') {
    return stream.push(item.name)
  }

  this.s3.getObject(item.params, function parse(err, data) {
    if (err) return stream.emit('error', err)

    try {
      data = JSON.parse(data.Body.toString())
    } catch (err) {
      return stream.emit('error', err)
    }

    if (!stream.options.keys && typeof stream.options.keys !== 'undefined') {
      return stream.push(data)
    }

    stream.push({key: item.name, value: data})
  })
}
