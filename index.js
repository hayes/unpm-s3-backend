var EE = require('events').EventEmitter
var join = require('path').join
var qs = require('querystring')
var aws = require('aws-sdk')
var through = require('through')
var PassThrough = require('stream').PassThrough


var params = {
  params: {
    Bucket: 'unpm-s3-backend-test'
  }
}

module.exports = S3Backend

function S3Backend(options) {
  if (!(this instanceof S3Backend)) {
    return new S3Backend(options)
  }

  EE.call(this)

  this.options = options || {}
  this.s3 = new aws.S3(options.params)
}

S3Backend.prototype = Object.create(EE.prototype)
S3Backend.prototype.constructor = S3Backend

S3Backend.prototype.getUser = getter('users', '~/users/')
S3Backend.prototype.setUser = setter('users', '~/users/', 'setUser', 'getUser')
S3Backend.prototype.removeUser = remover('users', '~/users/', 'removeUser', 'getUser')
S3Backend.prototype.getMeta = getter('meta', '')
S3Backend.prototype.setMeta = setter('meta', '', 'setMeta', 'getMeta')
S3Backend.prototype.removeMeta = remover('meta', '', 'removeMeta', 'getMeta')
S3Backend.prototype.get = getter('store', '~/store/')
S3Backend.prototype.set = setter('store', '~/store/', 'set', 'get')
S3Backend.prototype.remove = remover('store', '~/store/', 'remove', 'get')

S3Backend.prototype.createUserStream = function getUser(options) {
  throw new Error('Not Implemented')
}

S3Backend.prototype.createMetaStream = function getUser(options) {
  throw new Error('Not Implemented')
}

S3Backend.prototype.createStream = function getUser(options) {
  throw new Error('Not Implemented')
}

S3Backend.prototype.getTarball = function getTarball(_name, version) {
  var stream = new PassThrough()
  var name = escape(_name)
  var params = {}
  if (this.options.tarballs) {
    params.Bucket = this.options.tarballs.Bucket
    name = (this.options.tarballs.prefix || '') + name
  }

  params.Key = name + '@' + version + '.tgz'

  this.s3.getObject(params, function write(err, data) {
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

  params.Key = name + '@' + version + '.tgz'
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

  params.Key = name + '@' + version + '.tgz'

  this.s3.deleteObject(params, function removed(err, data) {
    if (err) return done(err, null)
    done(null)
  })
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

function escape(name) {
  return name.replace('/', '%2f')
}

function unescape(name) {
  return name.replace('%2f', '/')
}
/*

  function streamAll(dir) {
    return function stream_data(options) {
      return jrs(dir, options).pipe(unescape_stream(options))
    }
  }

  function getTarball(name, version) {
    return fs.createReadStream(
        join(tarballs_dir, qs.escape(name) + '@' + version + '.tgz')
    )
  }

  function setTarball(name, version) {
    return fs.createWriteStream(
        join(tarballs_dir, qs.escape(name) + '@' + version + '.tgz')
    )
  }

  function removeTarball(name, version, callback) {
    fs.unlink(join(tarballs_dir, qs.escape(name) + '@' + version + '.tgz'), callback)
  }
}

function unescape_stream(options) {
  if(options && !options.keys && typeof options.keys !== 'undefined') {
    return through()
  }

  return through(function unescape(data) {
    if(typeof data === 'object') {
      data.key = qs.unescape(data.key)
      return this.queue(data)
    }

    return this.queue(qs.unescape(data))
  })
}
function noop() {}
*/
