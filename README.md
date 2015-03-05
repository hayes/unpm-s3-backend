# μnpm-s3-backend

store unpm meta-data and tarballs in s3

## Example

```javascript
var backend = require('unpm-s3-backend')
var unpm = require('unpm')

// Defaults
var options = {
  // passed to new aws.S3(params) see aws documentation.
  params: {params: {Bucket: 'no-default-bucket-set'}},
  users: {
    prefix: '~/users/'
    Bucket: 'user-bucket' // no default
  },
  meta: {
    prefix: ''
    Bucket: 'meta-bucket' // no default
  },
  store: {
    prefix: '~/store/'
    Bucket: 'store-bucket' // no default
  },
  tarballs: {
    prefix: ''
    Bucket: 'tarball-bucket' // no default
  },
}

unpm({backend: backend(options)}).server.listen(8000)
```

### Example S3 bucket policy

allow installs even when unpm is not running
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "PublicReadForGetBucketObjects",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"NotResource": [
				"arn:aws:s3:::unpm-bucket/~/*",
				"arn:aws:s3:::unpm-bucket/~/"
			]
		}
	]
}
```

## License

MIT
