var test = require('unpm-backend-test')
var s3backend = require('../')

var backend = s3backend({
  s3: {params: {Bucket: 'unpm-s3-backend-test'}}
})

test(backend)
